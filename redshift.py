# Amazon redshift Module
import pandas.io.sql as sqlio
import boto

import boto.s3 
from boto.s3.key import Key

# View Listed tables in AWS Redsfhit #
def awstbl(conn, schemaName):
	qry = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schemaName + "'"
	tbl = sqlio.read_frame(qry, conn, coerce_float=True, params=None)
	return tbl

# View Columns for Table #
def awscol(conn, schema, tableName):
	qry = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name ='" + tableName + "' and table_schema = '" + schema + "'"
	tbl = sqlio.read_frame(qry, conn, coerce_float=True, params=None)
	return tbl
	
# Execute a Query #
def awsqry(conn, qry):
	tbl = sqlio.read_frame(qry, conn, coerce_float=True, params=None)
	return tbl

# Query Create, Drop Tables #
def awsexc(conn, qry):
 cur = conn.cursor()
 cur.execute(qry)
 conn.commit()
 return
 
# Function to upload data to S3 bucket and upload to Redshift Tables #
def s3upload(conn, access_key, secret_key, bucket_name, bucket_to, csv_from, csv_name, target_table):
 AWS_ACCESS_KEY_ID = access_key
 AWS_SECRET_ACCESS_KEY = secret_key
 bucket_name = bucket_name
 b_conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
 testfile = csv_from + csv_name
 bucket = b_conn.get_bucket(bucket_name)
 print """uploading %s to Amazon s3 bucket %s""" % (testfile,bucket_name) 
 k = Key(bucket)
 k.key = bucket_to
 k.set_contents_from_filename(testfile)
 cur = conn.cursor()
 st1 = "copy " + target_table + " from 's3://" + bucket_name + "/" + bucket_to + "' CREDENTIALS 'aws_access_key_id=" + access_key
 st2 = ";aws_secret_access_key=" + secret_key + "' maxerror 500 IGNOREHEADER 1 ACCEPTINVCHARS blanksasnull emptyasnull fillrecord DELIMITER ','"
 str = st1 + st2
 re = cur.execute(str)
 conn.commit()
 cur.close()
 print re
 return

