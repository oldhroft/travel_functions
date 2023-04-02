spec=$1

base_name=$(basename ${1})

IFS='.' read -r -a array <<< "$base_name"
name=${array[0]}

echo "Updating API Gateway $name"

yc serverless api-gateway update \
    --name $name \
    --spec $spec