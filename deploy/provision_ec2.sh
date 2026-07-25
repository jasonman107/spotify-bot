#!/usr/bin/env bash
# One-shot EC2 provisioning for the spotify-bot paper-trading stack (eu-west-1).
#
# Works with a limited IAM user: no ec2:CreateKeyPair (SSH key is injected via
# cloud-init) and no ec2:AllocateAddress (uses the auto-assigned public IP,
# which changes on stop/start — update the EC2_HOST secret then). Safe to
# re-run after a partial failure; reuses existing resources by name.
#
# Usage: ./deploy/provision_ec2.sh
set -euo pipefail

REGION=eu-west-1
NAME=spotify-bot-paper
SG_NAME=$NAME-sg
INSTANCE_TYPE=t3.small
KEY_FILE=deploy/$NAME-key.pem

echo "== local SSH key (injected via cloud-init, no AWS key pair needed) =="
if [ ! -f "$KEY_FILE" ]; then
  ssh-keygen -t ed25519 -N "" -C "$NAME" -f "${KEY_FILE%.pem}"
  mv "${KEY_FILE%.pem}" "$KEY_FILE"
  chmod 600 "$KEY_FILE"
  echo "created $KEY_FILE"
fi
PUBKEY=$(cat "${KEY_FILE%.pem}.pub")

echo "== security group (SSH only) =="
VPC_ID=$(aws ec2 describe-vpcs --region $REGION \
  --query 'Vpcs[?IsDefault].VpcId' --output text)
SG_ID=$(aws ec2 describe-security-groups --region $REGION \
  --filters Name=group-name,Values=$SG_NAME Name=vpc-id,Values=$VPC_ID \
  --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)
if [ "$SG_ID" = "None" ] || [ -z "$SG_ID" ]; then
  SG_ID=$(aws ec2 create-security-group --region $REGION \
    --group-name $SG_NAME --description "spotify-bot paper trading" \
    --vpc-id "$VPC_ID" --query 'GroupId' --output text)
  aws ec2 authorize-security-group-ingress --region $REGION --group-id "$SG_ID" \
    --protocol tcp --port 22 --cidr 0.0.0.0/0
fi
echo "sg: $SG_ID"

echo "== AMI (Ubuntu 24.04 LTS amd64) =="
AMI=$(aws ec2 describe-images --region $REGION --owners 099720109477 \
  --filters 'Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*' \
            'Name=state,Values=available' \
  --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text)
echo "ami: $AMI"

echo "== instance =="
EXISTING=$(aws ec2 describe-instances --region $REGION \
  --filters "Name=tag:Name,Values=$NAME" \
            "Name=instance-state-name,Values=pending,running" \
  --query 'Reservations[0].Instances[0].InstanceId' --output text 2>/dev/null || true)
if [ "$EXISTING" != "None" ] && [ -n "$EXISTING" ]; then
  INSTANCE_ID=$EXISTING
  echo "instance already running: $INSTANCE_ID"
else
  USER_DATA=$(cat <<EOF
#!/bin/bash
set -e
mkdir -p /home/ubuntu/.ssh
echo "$PUBKEY" >> /home/ubuntu/.ssh/authorized_keys
chown -R ubuntu:ubuntu /home/ubuntu/.ssh
chmod 600 /home/ubuntu/.ssh/authorized_keys
apt-get update
apt-get install -y ca-certificates curl
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu noble stable" > /etc/apt/sources.list.d/docker.list
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
usermod -aG docker ubuntu
mkdir -p /opt/spotify-bot/data
chown -R ubuntu:ubuntu /opt/spotify-bot
EOF
)
  INSTANCE_ID=$(aws ec2 run-instances --region $REGION \
    --image-id "$AMI" --instance-type $INSTANCE_TYPE \
    --security-group-ids "$SG_ID" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3"}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NAME}]" \
    --user-data "$USER_DATA" \
    --query 'Instances[0].InstanceId' --output text)
  echo "launched: $INSTANCE_ID"
  aws ec2 wait instance-running --region $REGION --instance-ids "$INSTANCE_ID"
fi

# No ec2:AllocateAddress permission — use the auto-assigned public IP.
IP=$(aws ec2 describe-instances --region $REGION --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

echo
echo "==================================================================="
echo "instance : $INSTANCE_ID ($INSTANCE_TYPE, $REGION)"
echo "public IP: $IP"
echo "ssh      : ssh -i $KEY_FILE ubuntu@$IP"
echo
echo "next:"
echo "  1. ./deploy/bootstrap_data.sh $IP     # .env + model/backfill data"
echo "  2. gh secret set EC2_HOST --body $IP"
echo "     gh secret set EC2_SSH_KEY < $KEY_FILE"
echo "  3. push to main (or gh workflow run deploy.yml) to deploy"
echo "==================================================================="
