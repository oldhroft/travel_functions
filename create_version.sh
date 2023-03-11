#! /bin/bash
echo "configuring $1"

echo "Removing old file"
rm -rf "$1.zip"

echo "Exporting secrets"
source secrets.sh

if [[ "$1" == "" ]]
then 
    echo "No folder specified"
    exit 1
fi

echo "Zipping objects of $1"
zip -r -j $1.zip $1/*

echo "Deleting old project in S3"
aws --endpoint-url=$AWS_ENDPOITNT_URL \
s3api delete-object \
--bucket parsing \
--key functions/$1.zip \

echo "Uploading new version to S3"
aws --endpoint-url=$AWS_ENDPOITNT_URL \
s3api put-object \
--body "$1.zip" \
--bucket parsing \
--key functions/$1.zip \

echo "Removing zip file"
rm -rf "$1.zip"

echo "Creating function"
yc serverless function version create \
  --function-name=$1 \
  --runtime python39 \
  --entrypoint index.handler \
  --memory 256m \
  --execution-timeout 120s \
  --package-bucket-name parsing \
  --package-object-name functions/$1.zip \
  --service-account-id $SERVICE_ACCOUNT_ID \
  --environment "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY,AWS_ENDPOITNT_URL=$AWS_ENDPOITNT_URL,AWS_REGION_NAME=$AWS_REGION_NAME,YDB_DATABASE=$YDB_DATABASE,YDB_ENDPOINT=$YDB_ENDPOINT"