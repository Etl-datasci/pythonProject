from pyspark.sql import *
from pyspark.sql.functions import *
import boto3

#
ec2 = boto3.resource('ec2', region_name='ap-south-1',
                     aws_access_key_id="AKIA4XWSRVGJZSAF2LUP",
                     aws_secret_access_key="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")

# create the ubuntu instance code
instance = ec2.create_instances(
    ImageId='ami-0f5ee92e2d63afc18',
    MinCount=1,
    MaxCount=1,
    InstanceType='t2.micro',
    KeyName="connkey"
)

for instance in ec2.instances.all():
    print("Id:{0} \nTag :{1} \nType :{2} \nnAMI :{3} \nState :{4}".format(instance.id,
                                                                          instance.tags, instance.instance_type,
                                                                          instance.image.id, instance.state))
ids = ['i-0208df69077c82ddf', 'i-0411afc8d646bdc70', 'i-01cb7be45372751ae', 'i-0fab37e9a548e5919']
response = ec2.instances.filter(InstanceIds=ids).terminate()
print("Response: ", response)

s3 = boto3.client('s3')

response = s3.list_buckets()
buckets= response["Buckets"]

for bucket in buckets:
    print(bucket["Name"])
