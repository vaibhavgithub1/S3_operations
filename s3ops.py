import boto3
import os

bucketName="sampletestvaibhav"

s3client = boto3.client('s3', aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                              aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
                              region_name=os.environ.get('AWS_DEFAULT_REGION'))

def addobjecttos3():
    try:
        currentWorkDir=os.getcwd()
        for i in range(1001,2000):
            filepath = currentWorkDir+"/Temp"+"/obj_"+str(i)+".txt"
            f = open(filepath, "a")
            f.write("Now the file has more content!")
            f.close()
            s3client.put_object(Body=filepath, Bucket=bucketName, Key="file_"+str(i)+".txt", Tagging="myfile="+str(i),
                                Metadata={'samplemeta': 'obj_'+str(i)})
    except Exception as e:
        print("Error Occured ", e)



def fetchall():
    try:
        # list all object
        response = s3client.list_objects_v2(
            Bucket=bucketName
        )
        currentWorkDir = os.getcwd()
        for obj in response["Contents"]:
            print(obj["Key"])
            filepath = currentWorkDir+"/Temp/"
            s3client.download_file(bucketName, obj["Key"],filepath+str(obj["Key"]))

        # res=s3client.get_object(Bucket=bucketName,IfMatch="myfile")
        # print(res)
    except Exception as e:
        print("Error Occured ", e)

def deleteobject():
    # get the object name with specific metadata
    metadata_key="x-amz-meta-samplemeta"
    metadata_value="obj_102"

    res = s3client.delete_object(Bucket=bucketName,Key="file_10.txt")
    # delete the object with tag
