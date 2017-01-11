import boto3,botocore
client = boto3.client('iam')

rn = 'biglambda_role'
rp = 'biglambda_policy'

try:
    response = client.delete_role_policy(RoleName=rn,PolicyName=rp)
    print response
    print "Success: done deleting role policy"
except botocore.exceptions.ClientError as e:
    print "Error: {0}".format(e)
 
response = client.delete_role(RoleName=rn)
print response
print "Success: done deleting role"
