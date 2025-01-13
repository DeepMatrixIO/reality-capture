# Creating and submitting a Reality Modeling job
import os
import time

import reality_apis.CCS.context_capture_service as CCS
import reality_apis.DataTransfer.reality_data_transfer as DataTransfer

from reality_apis.DataTransfer.references import ReferenceTable
from reality_apis.CCS.ccs_utils import CCJobSettings, CCJobQuality, CCJobType
from reality_apis.iTwins.itwins import iTwinsApiWrapper, iTwinSettings, iTwinClass, iTwinSubClass, iTwinStatus
from reality_apis.utils import RealityDataType, JobState, ReturnValue

from config import client_id, secret
from token_factory.token_factory import ClientInfo, ServiceTokenFactory, AbstractTokenFactory


# return a service token factory
def service_token_factory():
    scope_set = {
        "itwin-platform"
    }
    client_info = ClientInfo(client_id, scope_set, secret=secret)
    return ServiceTokenFactory(client_info)


# creates iTwin with the given setting and returns the project id
def create_iTwins(token_factory: AbstractTokenFactory, iTwin_setting: iTwinSettings):
    response: ReturnValue = iTwinsApiWrapper(token_factory).create_iTwin(iTwin_setting)
    if response.is_error():
        print("Error creating iTwin:", response.error)
        exit(1)
    return response.value


# return a data transfer object with a progress hook
def data_transfer_with_progress_hook(token_factory: AbstractTokenFactory):
    # initializing data transfer
    data_transfer = DataTransfer.RealityDataTransfer(token_factory)
    # adding hook to follow upload and download status
    data_transfer.set_progress_hook(DataTransfer.example_hook)
    print("Data transfer initialized")
    return data_transfer


# return a cc service object
def init_cc_service(token_factory: AbstractTokenFactory):
    # initializing cc service
    service_cc = CCS.ContextCaptureService(token_factory)
    print("Service initialized")
    return service_cc


# upload data to the cloud and return the reference table
def upload_data(data_transfer: DataTransfer.RealityDataTransfer, project_id, cc_image_collections, cc_orientations,
                cc_image_collections_name, cc_orientations_name, output_path):
    # creating reference table and uploading ccimageCollection, ccOrientations if necessary (not yet on the cloud)
    references = ReferenceTable()
    references_path = os.path.join(output_path, "test_references_python.txt")
    if os.path.isfile(references_path):
        print("Loading preexistent references")
        ret = references.load(references_path)
        if ret.is_error():
            print("Error while loading preexisting references:", ret.error)
            exit(1)

    # upload ccimageCollection
    if not references.has_local_path(cc_image_collections):
        print(
            "No reference to CCimage Collections found, uploading local files to cloud"
        )
        ret = data_transfer.upload_reality_data(
            cc_image_collections,
            cc_image_collections_name,
            RealityDataType.CCImageCollection,
            project_id,
        )
        if ret.is_error():
            print("Error in upload:", ret.error)
            exit(1)
        ret = references.add_reference(cc_image_collections, ret.value)
        if ret.is_error():
            print("Error adding reference:", ret.error)
            exit(1)

    references.save(references_path)
    # upload ccorientations
    if not references.has_local_path(cc_orientations):
        print("No reference to cc orientations found, uploading local files to cloud")
        ret = data_transfer.upload_ccorientation(
            cc_orientations, cc_orientations_name, project_id, references
        )
        if ret.is_error():
            print("Error in upload:", ret.error)
            exit(1)
        ret = references.add_reference(cc_orientations, ret.value)
        if ret.is_error():
            print("Error adding reference:", cc_orientations)
            exit(1)

    # saving references (so we don't need to re-upload afterwards)
    ret = references.save(references_path)
    if ret.is_error():
        print("Error saving references:", ret.error)
        exit(1)
    print("Checked data upload")
    return references


# create workspace and return workspace id
def create_workspace(service_cc, workspace_name, project_id):
    ret = service_cc.create_workspace(workspace_name, project_id)
    if ret.is_error():
        print("Error creating workspace:", ret.error)
        exit(1)
    print("WorkScape creation response", ret)
    return ret.value


# create job settings
def create_job_settings(references, cc_image_collections, cc_orientations, outputs: CCJobSettings.Outputs,
                        quality: CCJobQuality = CCJobQuality.MEDIUM):
    settings = CCJobSettings()
    settings.inputs = [
        references.get_cloud_id_from_local_path(cc_image_collections).value,
        references.get_cloud_id_from_local_path(cc_orientations).value,
    ]
    settings.outputs = outputs
    settings.mesh_quality = quality
    print("Settings created")
    return settings


# create job and return job id
def create_job(service_cc, workspace_id, job_name, settings: CCJobSettings):
    ret = service_cc.create_job(CCJobType.FULL, settings, job_name, workspace_id)
    if ret.is_error():
        print("Error in submit:", ret.error)
        exit(1)
    print("Created Job: ", ret)
    job_id = ret.value
    ret = service_cc.submit_job(job_id)
    if ret.is_error():
        print("Error in submit:", ret.error)
        exit(1)
    print("Job submission response: ", ret)
    print("JobID: ", job_id)
    return job_id


def tracking_job_progress(service_cc, job_id):
    while True:
        progress_ret = service_cc.get_job_progress(job_id)
        if progress_ret.is_error():
            print("Error while getting progress:", progress_ret.error)
            exit(1)
        job_progress = progress_ret.value
        if (
                job_progress.state == JobState.SUCCESS
                or job_progress.state == JobState.Completed
                or job_progress.state == JobState.Over
        ):
            break
        elif (
                job_progress.state == JobState.ACTIVE
                or job_progress.state == JobState.Running
        ):
            print(f"Progress: {str(job_progress.progress)}%, step: {job_progress.step}")
        elif job_progress.state == JobState.CANCELLED:
            print("Job cancelled")
            exit(0)
        elif job_progress.state == JobState.FAILED:
            print("Job Failed")
            print(f"Progress: {str(job_progress.progress)}%, step: {job_progress.step}")
            exit(1)
        time.sleep(60)
    print("Job done")


def download_outputs(data_transfer: DataTransfer.RealityDataTransfer, service_cc: CCS.ContextCaptureService, project_id,
                     job_id, output_path):
    print("Retrieving outputs ids")
    ret = service_cc.get_job_properties(job_id)
    if ret.is_error():
        print("Error while getting properties:", ret.error)
        exit(1)
    final_settings = ret.value.job_settings
    print("Downloading outputs")

    output_types = {key: value for key, value in final_settings.outputs.__dict__.items() if value is not None}

    for output_type, output_id in output_types.items():
        if output_id is not None:
            ret = data_transfer.download_reality_data(output_id, output_path, project_id)
            if ret.is_error():
                print(f"Error while downloading {output_type} output:", ret.error)
            print(f"Downloaded {output_type} output:", ret)
    print("Downloads Completed")


def main():
    cc_image_collections = r"path to your image folder"
    cc_orientations = r"path to the folder where your ccorientation file is"
    output_path = r"path to the folder where you want to save outputs"

    job_name = "Reality Modeling job"
    workspace_name = "Reality Modeling workspace"
    cc_image_collections_name = "Photos"
    cc_orientations_name = "cc orientations"

    project_settings = iTwinSettings()
    project_settings.name = "Custom Project Name"
    project_settings.iTwin_class = iTwinClass.THING
    project_settings.iTwin_subclass = iTwinSubClass.ASSET
    project_settings.status = iTwinStatus.ACTIVE
    project_settings.iTwin_number = "Unique Id (string) inside org"
    project_settings.display_name = "Display name"
    # Optional settings
    project_settings.data_center_location = "East US"  # Default is "East US"
    project_settings.description = "Optional Description"
    project_settings.iTwin_type = "Optional Custom type name"
    project_settings.geographic_location = "Optional Geographic location"

    # Job Outputs, more outputs can be added (see CCJobSettings.Outputs)
    job_outputs = CCJobSettings.Outputs()
    job_outputs.cesium_3D_tiles = "3DTiles_name"
    job_outputs.obj = "obj_name"
    job_outputs.las = "las_name"
    job_outputs.orthophoto_DSM = "orthophoto_DSM_name"


    # create iTwin, upload data, create workspace, create job, track job progress and download outputs
    token_factory = service_token_factory()
    project_id = create_iTwins(token_factory, project_settings)
    data_transfer = data_transfer_with_progress_hook(token_factory)
    service_cc = init_cc_service(token_factory)
    references = upload_data(data_transfer, project_id, cc_image_collections, cc_orientations,
                             cc_image_collections_name, cc_orientations_name, output_path)
    workspace_id = create_workspace(service_cc, workspace_name, project_id)
    settings = create_job_settings(references, cc_image_collections, cc_orientations, job_outputs)
    job_id = create_job(service_cc, workspace_id, job_name, settings)
    tracking_job_progress(service_cc, job_id)
    download_outputs(data_transfer, service_cc, project_id, job_id, output_path)


if __name__ == "__main__":
    main()
