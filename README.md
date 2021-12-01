# Data Engineering - Assignment 2 - Group 4
The repository for Group 4 Assignment 2 of Data Engineering at JADS 2021-2022.

This directory accompanies the report submitted. More information about the exact configuration can be found there.

## Replicating results
To replicate the results with this repository:
1. Clone repository to your machine (VM).
2. Add firewall rules if running on VM. See deployment/docker-compose.yml for ports.
3. For VM, replace the external IP of our machine with your own.
4. Replace usernames, BigQuery and GCS Bucket locations with your own username or location.
    BigQuery tables should also be created. SQL for this can be found in create_tables.txt.
5. Run the build and run the docker-compose deployment.
6. Use the admin.py script to create (and possibly delete) Kafka topics for data streaming.
7. Run the 4 Jupyter Notebooks saved as "backup" in this repo.
    These should be ran from within the spark-driver-app container.
    Check the logs of this container to access the JupyterLab instance running on the container.
8. To test streaming functionality, run the producer_demo.py.
    This program streams data to the Kafka container.
9. Data should now be reaching your BigQuery by a batch and stream pipeline.
