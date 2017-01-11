import boto3,json,botocore
client = boto3.client('iam')

trust_role = {
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}

rn='biglambda_role'
rp='biglambda_policy'
try:
    response = client.create_role(RoleName=rn,AssumeRolePolicyDocument=json.dumps(trust_role))
    print response
    print "Success: done creating role"
except botocore.exceptions.ClientError as e:
    print "Error: {0}".format(e)

with open('policy.json') as json_data:
    response = client.put_role_policy(RoleName=rn,PolicyName=rp,
        PolicyDocument=json.dumps(json.load(json_data))
    )
    print response
    print "Success: done adding inline policy to role"

