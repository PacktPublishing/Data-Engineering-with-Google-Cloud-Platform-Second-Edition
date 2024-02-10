gcloud sql instances create mysql-instance-source-2  \
--database-version=MYSQL_8_0 \
--tier=db-g1-small \
--region=us-central1 \
--root-password=$PASSWORD \
--availability-type=zonal \
--storage-size=10GB \
--storage-type=HDD \
--backup \
--enable-point-in-time-recovery
