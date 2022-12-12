import os
import yaml
import time
from azure.storage.blob import ContainerClient

#to read and complete the configurations on .yaml file
def load_config():
    dir_root= os.path.dirname(os.path.abspath(__file__))
    with open(dir_root+"/config.yaml","r") as f:
        return yaml.load(f,Loader=yaml.FullLoader)

#to read the file from the local folder
def get_file():
    #if the .txt file is stored in a different folder, the folder name should be specified here
    os.chdir('readings/')
    print(os.listdir())
    with os.scandir() as entries:
        for entry in entries:
            if entry.is_file() and not entry.name.startswith('.'):
                yield entry

#to upload the file to azure storage
def upload(files,connectionstring,containername):
    container_client = ContainerClient.from_connection_string(connectionstring,containername)
    print('upload is in progress')

    for file in files:
        blob_client = container_client.get_blob_client(file.name)
        with open(file.path,"rb") as data:
            blob_client.upload_blob(data)
            print("upload completed")
            # time.sleep(10)
            data.close()

            #to remove the file from local folder once it's pushed to azure
            os.remove(file)
            print(f'{file.name} is moved to azure and removed from local directory!')

#     print("\nListing blobs...")

# # List the blobs in the container
#     blob_list = container_client.list_blobs()
#     for blob in blob_list:
#         print("\t" + blob.name)

config= load_config()
filelist = get_file()
upload(filelist,config["azure_storage_connectionstring"],config["text_container_name"])