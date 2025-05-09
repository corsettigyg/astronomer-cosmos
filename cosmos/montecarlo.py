import os
import tempfile
from airflow.io.path import ObjectStoragePath
from pycarlo.core import Client, Session, Query
from pycarlo.features.dbt.dbt_importer import DbtImporter
from cosmos.log import get_logger
from cosmos.exceptions import CosmosValueError

logger = get_logger(__name__)

def get_resource_id(client):
    """Get the resource ID of the first warehouse connected to the user's account"""
    query = Query()
    query.get_user().account.warehouses.__fields__("name", "connection_type", "uuid")
    warehouses = client(query).get_user.account.warehouses
    warehouse_list = []
    if len(warehouses) > 0:
        for val in warehouses:
            warehouse_list.append(val.uuid)
    else:
        logger.error("no warehouses connected ! Please check your Monte Carlo account.")
    return warehouse_list

def montecarlo_import_cloud_artifacts(
        context,
        mcd_id=None,
        mcd_token=None,
        project_name=None,
        job_name=None,
        bucket_name=None,
        resource_id=None,
        **kwargs
):
    """
    Airflow on_success_callback function that authenticates to Monte Carlo and sends dbt run artifacts.

    Args:
        context: The Airflow context passed to the callback
        mcd_id: Monte Carlo token user ID
        mcd_token: Monte Carlo token value
        project_name: Project name (perhaps a logical group of dbt models)
        job_name: Job name (required - logical sequence of dbt executions)
        s3_bucket: S3 bucket name where dbt artifacts are stored
        connection_id: Identifier of warehouse or lake connection (required if you have multiple connections)
    """
    if not mcd_id or not mcd_token:
        raise CosmosValueError("Monte Carlo credentials are required to authenticate with MonteCarlo!")

    if kwargs["aws_conn_id"]:
        conn_id = kwargs["aws_conn_id"]
    elif kwargs["gcp_conn_id"]:
        conn_id = kwargs["gcp_conn_id"]
    elif kwargs["azure_conn_id"]:
        conn_id = kwargs["azure_conn_id"]
    elif not conn_id:
        raise CosmosValueError("Connection ID to the cloud provider is required")
    
    # create a client with the provided credentials
    client = Client(session=Session(mcd_id=mcd_id, mcd_token=mcd_token))

    dbt_importer = DbtImporter(mc_client=client)

    # Create a temporary directory to store downloaded artifacts
    with tempfile.TemporaryDirectory() as temp_dir:

        cloud_path_prefix = (
            f"{context['dag'].dag_id}"
            f"/{context['run_id']}"
            f"/{context['task_instance'].task_id}"
            f"/{context['task_instance']._try_number}"
            f"/target"
        )

        # Define the artifact filenames to download. dbt.log is just a placeholder for now, since cosmos
        # don't have a way to push logs to the cloud as of now.
        artifact_files = {
            "manifest.json": os.path.join(temp_dir, "manifest.json"),
            "run_results.json": os.path.join(temp_dir, "run_results.json"),
            "dbt.log": os.path.join(temp_dir, "dbt.log")  # placeholder
        }

        # Download the artifacts from S3
        for filename, local_path in artifact_files.items():
            # Construct the full S3 path based on the pattern
            cloud_path = f"{bucket_name}/{cloud_path_prefix}/{filename}"
            object_path = ObjectStoragePath(cloud_path, conn_id=conn_id)

            try:
                # Check if the file exists
                if object_path.exists():
                    logger.info(f"Downloading {filename} from {cloud_path}")
                    with open(local_path, 'wb') as local_file:
                        with object_path.open('rb') as cloud_file:
                            local_file.write(cloud_file.read())
                else:
                    logger.error(f"File {cloud_path} does not exist")
                    if filename in ["manifest.json", "run_results.json"]:
                        raise FileNotFoundError(f"Required file {filename} not found at {cloud_path}")
                    artifact_files[filename] = None  # Mark optional files as not available
            except Exception as e:
                logger.error(f"Error downloading {filename}: {str(e)}")
                if filename in ["manifest.json", "run_results.json"]:
                    raise
                artifact_files[filename] = None  # Mark optional files as not available

        logger.info("Sending dbt run artifacts to Monte Carlo")

        try:
            import_options = {
                "manifest_path": artifact_files["manifest.json"],
                "run_results_path": artifact_files["run_results.json"],
                "project_name": project_name,
                "job_name": job_name
            }

            # Add optional parameters if available
            if artifact_files.get("dbt.log"):
                import_options["logs_path"] = artifact_files["dbt.log"]

            # Use connection_id as resource_id if provided
            if resource_id:
                import_options["resource_id"] = resource_id
            else:
                first_resource_id = get_resource_id(client)[0]
                import_options["resource_id"] = first_resource_id

            dbt_importer.import_run(**import_options)
            logger.info("Successfully sent dbt run artifacts to Monte Carlo")
        except Exception as e:
            logger.info(f"Failed to send dbt run artifacts to Monte Carlo: {str(e)}")
            raise