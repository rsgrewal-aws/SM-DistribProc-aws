# sagemaker-aws

Create a bucket by name of sagemaker-crossaccnt-train -- or Change the DEF_BUCKET Variable
Create a folder prefix of data/finance/distrib-multi  -- or Change the PREFIX Variable



#### Generate Data
*Generate synthetic housing data -- with no Categorical columns for now -- 
    *SHARD is RANDOM INDEX and so everytime we run we get a new set of shard indexes and files
    *Generate Json File -- not included as yet
    *Generate Zip file - not included as yet
    
#### Experiments run
*Below are the Jupyter files with the experiements run -- 
    *Folder Shard -- We are sharding the data by folders and each folder has 1 csv file to be processed
    *TarBall Shard -- We are shariding the data by creating 1 TAR BALL for each of the unique data set to be processed
    *Manifest Shard -- We are sharding by the Manifest file which has the location for various files and data sets
        * For Manifest 5 different experiments were run which are documented 
        * While creating the Manifest file the S3 bucket had to be hard coded in that script. Please CHANGE to your bucket
    *Direct File shard -- Place al files directly under 1 folder in S3 and try to shard by s3
    
    
