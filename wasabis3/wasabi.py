import boto3
from time import sleep
import botocore.config
from formatter import indicators


class WasabiPurge:
    def __init__(self, ACCESS_KEY_ID, SECRET_ACCESS_KEY, ENDPOINT, BUCKET_NAME):
        self.accessKey = ACCESS_KEY_ID
        self.secretAccessKey = SECRET_ACCESS_KEY
        self.endPoint = ENDPOINT
        self.bucketName = BUCKET_NAME

        self.config = botocore.config.Config(
            retries=dict(max_attempts=10),
            connect_timeout=120,
            read_timeout=120,
            max_pool_connections=90,
        )

        self.S3Resource = boto3.resource(
            "s3",
            endpoint_url=self.endPoint,
            aws_access_key_id=self.accessKey,
            aws_secret_access_key=self.secretAccessKey,
            config=self.config,
        )

        self.S3Client = boto3.resource(
            "s3",
            endpoint_url=self.endPoint,
            aws_access_key_id=self.accessKey,
            aws_secret_access_key=self.secretAccessKey,
            config=self.config,
        )

        self.bucket = self.S3Resource.Bucket(self.bucketName)

    def deleteNonCurrents(self):
        print(
            f"{indicators.HEADER}You have chosen to delete Non-current Objects!{indicators.ENDC}"
        )
        bucket = self.S3Resource.Bucket(self.bucketName)
        counter = 1
        paginator = self.S3Resource.meta.client.get_paginator("list_object_versions")

        print(f"{indicators.HEADER}Script is starting. Please wait...{indicators.ENDC}")
        for page in paginator.paginate(Bucket=self.bucketName):
            objects_to_delete = []
            for version in page.get("Versions", []):
                if version["IsLatest"] == False:
                    objects_to_delete.append(
                        {"VersionId": version["VersionId"], "Key": version["Key"]}
                    )
                if len(objects_to_delete) == 50000:
                    bucket.delete_objects(Delete={"Objects": objects_to_delete})
                    for i in range(len(objects_to_delete)):
                        print(
                            f"\r{indicators.OKGREEN}Batch Number [{counter}] <<<--->>> Deleting non-current objects: {i + 1} out of {len(objects_to_delete)}{indicators.ENDC}",
                            end="",
                        )
                        sleep(0.01)
                    objects_to_delete = []
                    counter += 1
            if len(objects_to_delete) > 0:
                bucket.delete_objects(Delete={"Objects": objects_to_delete})
                for i in range(len(objects_to_delete)):
                    print(
                        f"\r{indicators.OKGREEN}Batch Number [{counter}] <<<--->>> Deleting non-current objects: {i + 1} out of {len(objects_to_delete)}{indicators.ENDC}",
                        end="",
                    )
                    sleep(0.01)
                objects_to_delete = []
                counter += 1

    def deleteObjects(self):
        # print(f"{indicators.HEADER}You have chosen to delete Objects!{indicators.ENDC}")
        # Delete objects in batches of 50K
        counter = 1
        objects_to_delete = []
        for obj in self.bucket.objects.all():
            objects_to_delete.append({"Key": obj.key})
            if len(objects_to_delete) == 50000:
                self.bucket.delete_objects(Delete={"Objects": objects_to_delete})
                for i in range(len(objects_to_delete)):
                    print(
                        f"\r{indicators.OKGREEN}Batch Number [{counter}] <<<--->>> Deleting objects: {i + 1} out of {len(objects_to_delete)}{indicators.ENDC}",
                        end="",
                    )
                    sleep(0.01)
                objects_to_delete = []
                counter += 1
        if len(objects_to_delete) > 0:
            self.bucket.delete_objects(Delete={"Objects": objects_to_delete})
            for i in range(len(objects_to_delete)):
                print(
                    f"\r{indicators.OKGREEN}Batch Number [{counter}] <<<--->>> Deleting objects: {i + 1} out of {len(objects_to_delete)}{indicators.ENDC}",
                    end="",
                )
                sleep(0.01)
            counter += 1

        # Delete the bucket
        # bucket.delete()
        print("\nObjects deleted successfully!")

    def deleteVersionsandObjects(self):

        self.deleteNonCurrents()
        self.deleteObjects()

    def purgeDeleteMarkers(self):
        print(
            f"{indicators.HEADER}You have chosen to purge all delete markers (DMs)!{indicators.ENDC}"
        )
        bucket_name = self.bucketName

        # Paginate through all the delete markers in the bucket
        delete_markers = []

        paginator = self.S3Resource.meta.client.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=bucket_name):
            for version in page.get("Versions", []):
                if version.get("IsDeleteMarker"):
                    delete_markers.append(
                        {
                            "VersionId": version.get("VersionId"),
                            "Key": version.get("Key"),
                        }
                    )

                    # Delete a batch of 50000 delete markers
                    if len(delete_markers) == 50000:
                        for delete_marker in delete_markers:
                            self.S3Client.delete_object(
                                Bucket=bucket_name,
                                Key=delete_marker["Key"],
                                VersionId=delete_marker["VersionId"],
                            )
                        delete_markers = []

        # Delete any remaining delete markers
        for delete_marker in delete_markers:
            self.S3Resource.delete_object(
                Bucket=bucket_name,
                Key=delete_marker["Key"],
                VersionId=delete_marker["VersionId"],
            )

        print(
            f"{indicators.OKBLUE}Delete Markets Successfully Purged from {self.bucketName}{indicators.OKBLUE}"
        )

    def deleteBucket(self):
        bucket = self.S3Resource.Bucket(self.bucketName)

        # Ensure nothing else remains
        bucket.objects.all().delete()

        # Delete the bucket
        bucket.delete()

        print(f"{indicators.HEADER}Bucket deletion successful{indicators.ENDC}")


if __name__ == "__main__":
    from initializer import initialize

    WasabiClient = initialize()

    print(
        f"{indicators.OKGREEN}Choose from the following list of operations to run on your Wasabi bucket: {WasabiClient.bucketName}"
    )
    print(
        f"\t{indicators.OKMAGENTA}Operation 1: Delete Non-Current Objects {indicators.ENDC}"
    )
    print(
        f"\t{indicators.OKMAGENTA}Operation 2: Delete Current and Non-Current Objects {indicators.ENDC}"
    )
    print(
        f"\t{indicators.OKMAGENTA}Operation 3: Purge Delete Markers {indicators.ENDC}"
    )
    print(f"\t{indicators.OKMAGENTA}Operation 4: Delete Bucket {indicators.ENDC}")

    acceptableOptions = [1, 2, 3, 4]
    userSelection = int(
        input(
            f"{indicators.WARNING}Select Operation (Number 1-4) : {indicators.ENDC}"
        ).strip()
    )

    if userSelection not in acceptableOptions:
        print(f"{indicators.FAIL}Wrong Option Selected :( Try again!{indicators.ENDC}")
    else:
        if userSelection == 1:
            WasabiClient.deleteNonCurrents()
        elif userSelection == 2:
            for i in range(4):
                WasabiClient.deleteVersionsandObjects()
        elif userSelection == 3:
            WasabiClient.purgeDeleteMarkers()
        elif userSelection == 4:
            WasabiClient.deleteBucket()
