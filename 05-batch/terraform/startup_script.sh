#!/bin/bash
set -euo pipefail

HOME="/home/ubuntu"
LOG_FILE="$HOME/startup_script.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "[$(date)] Startup script started."

# System updates
sudo apt-get update -y
sudo apt-get upgrade -y
sudo apt-get install -y python3 python3-pip git docker.io curl wget

# Install Java
sudo apt-get install -y openjdk-11-jdk

# Install Miniconda
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
bash miniconda.sh -b -p "$HOME/miniconda"
echo "export PATH=\"$HOME/miniconda/bin:\$PATH\"" >> .bashrc

# Install Spark
SPARK_URL="https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop3.tgz"
wget "$SPARK_URL" -O spark.tgz
tar -xzf spark.tgz -C "$HOME"
echo "export SPARK_HOME=\"$HOME/spark-${spark_version}-bin-hadoop3\"" >> .bashrc
echo "export PATH=\"\$SPARK_HOME/bin:\$PATH\"" >> .bashrc

# Docker configuration
sudo usermod -aG docker ubuntu
sudo systemctl enable docker
sudo systemctl start docker

# Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/${docker_compose_version}/docker-compose-$(uname -s)-$(uname -m)" \
  -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Python environment
pip3 install --upgrade pip
pip3 install pyspark==${spark_version} google-cloud-storage jupyter

# Data download
mkdir -p "$HOME/data"
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv -O "$HOME/data/taxi_zone_lookup.csv"

# Cleanup
rm miniconda.sh spark.tgz

echo "[$(date)] Startup script completed successfully."
touch "$HOME/startup_script_completed"
chown -R ubuntu:ubuntu "$HOME"