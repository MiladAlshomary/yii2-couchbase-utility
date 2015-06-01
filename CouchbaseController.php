<?php
/**
 * Couchbase controller to handle some features :
 * 1- importing table into couchbase
 * 2- cron job to keep pulling new/updated records from table into couchbase
 *
 * @auther : MiladAlsh
 * Date: 06/05/15
 */
namespace console\controllers;
use yii;
use yii\db\Query;

class CouchbaseController extends \yii\console\Controller
{

	private $_cluster;

	/**
	* @param cbConnection string component name ( see https://github.com/MiladAlshomary/couchbase-yii2)
	* @param sqlConnection string component name , 
	* @param table string name of table to be imported
	* @param bucket_name string is the name of bucket to export data to, 
	* @param fields is array of key/value to map fields into specific names
	* @param patch_size integer the size of each patch from database
	* @param sleep integer number of secondes to sleep between each patch
	* This function will import your table into couchbase bucket in patch mode
	**/
	public function actionImport($sqlConnection, $cbConnection, $table, $bucket_name, $fields = null, $patch_size=1000, $offset = 0, $sleep = 5, $use_table_askey = 1) {
		$db = Yii::$app->{$sqlConnection};
		$cb = Yii::$app->{$cbConnection};
		
		if($db == null) {
			echo 'sqlConnection is not defiend'.PHP_EOL;
			return;
		}

		if($cb == null) {
			echo 'cbConnection is not defiend'.PHP_EOL;
			return;
		}

		$cb_bucket = $cb->getBucket($bucket_name);
		if($cb_bucket == null) {
			echo 'Couchbase Bucket cant be retrieved'.PHP_EOL;
			return;
		}

		//set time out
		$cb->setTimeOut(100000);

		$query = new Query;

		if(empty($fields) || $fields == 'all') {
			$fields = '*';
		}

		//load first batch
		$query->select($fields)->from($table)->orderBy('id')->limit($patch_size)->offset($offset);
		$rows = $query->all($db);
		while (!empty($rows)) {
			foreach ($rows as $document) {
				$offset ++;
				//push document into couchbase
				$document['cb_type'] = $table;
				if($use_table_askey == 1) {
					$key = $table . '_' . $document['id'];
				} else {
					$key = ''. $document['id'];
				}

				if($this->exists($cb_bucket, $key)){
					$cb_bucket->replace($key ,$document);
					echo 'update document with id : ' . $key .PHP_EOL;
				} else {
					$cb_bucket->insert($key ,$document);
					echo 'insert document with id : ' . $key .PHP_EOL;
				}
			}

			//sleep for amount of time
			try {
				echo 'sleeping for :' . $sleep . PHP_EOL;
				sleep($sleep);
			} catch(Exception $ex) {
				echo 'failed to sleep..' . PHP_EOL;
			}

			//load another batch
			$query->select($fields)->from($table)->orderBy('id')->limit($patch_size)->offset($offset);
			$rows = $query->all($db);
		}
		
		//pushing now to be the last_sync_time of the table into couchbase bucket
		$now = time();
		$cb_bucket->insert($table . '_last_sync_time', ['time' => $now]);
		
		echo 'Ohhh life is bigger than you and you are not me ... Done :) '. PHP_EOL;
		
	}


	/**
	* @param cbConnection string component name ( see https://github.com/MiladAlshomary/couchbase-yii2)
	* @param sqlConnection string component name , 
	* @param table string name of table to be imported
	* @param bucket_name string is the name of bucket to export data to
	* @param sync_field string is the name of time field you need to sync accordingly 
	* @param fields is array of key/value to map fields into specific names
	* This function will sync the table into couchbase bucket based on the sync_field
	**/
	public function actionSync($sqlConnection, $cbConnection, $table, $bucket_name, $sync_field, $fields = null, $use_table_askey = 1) {
		$db = Yii::$app->{$sqlConnection};
		$cb = Yii::$app->{$cbConnection};
		
		$offset = 0;
		$limit  = 5000;

		if($db == null) {
			echo 'sqlConnection is not defiend'.PHP_EOL;
			return;
		}

		if($cb == null) {
			echo 'cbConnection is not defiend'.PHP_EOL;
			return;
		}

		$cb_bucket = $cb->getBucket($bucket_name);
		if($cb_bucket == null) {
			echo 'Couchbase Bucket cant be retrieved'.PHP_EOL;
			return;
		}

		$cb->setTimeOut(100000);

		//retrieve couchbase timestamp for the last sync
		$last_sync_date = $this->getLastSyncTime($table, $cb_bucket);	
		$query = new Query;

		if(empty($fields) || $fields == 'all') {
			$fields = '*';
		}

		//load first batch
		$query->select($fields)->from($table)->where($sync_field . ' > \'' . date('Y-m-d H:i:s',$last_sync_date) . '\'')->limit($limit)->offset($offset);
		$rows = $query->all($db);
		while (!empty($rows)) {
			foreach ($rows as $document) {
				$offset ++;
				//push document into couchbase
				$document['cb_type'] = $table;
				if($use_table_askey == 1) {
					$key = $table . '_' . $document['id'];
				} else {
					$key = ''. $document['id'];
				}

				if($this->exists($cb_bucket, $key)){
					$cb_bucket->replace($key ,$document);
					echo 'update document with id : ' . $key .PHP_EOL;
				} else {
					$cb_bucket->insert($key ,$document);
					echo 'insert document with id : ' . $key .PHP_EOL;
				}
				echo 'pushing document with offset : ' . $offset .PHP_EOL;
			}

			//load another batch
			$query->select($fields)->from($table)->limit($limit)->offset($offset);
			$rows = $query->all($db);
		}
		
		//update the couchbase sync time stamp
		$cb_bucket->replace($table . '_last_sync_time', ['time' => time()]);
		
		echo 'Ohhh life is bigger than you and you are not me ... Done :) '. PHP_EOL;

	}

	/**
	*	table_name the table name you want to get the last_sync_time of it
	* bucket is the couchbase bucket where the table is sync to
	* if the table is preivously synched to the bucket it returnes the last sync time
	* if the table is not pushed to the bucket before the it returns now()
	**/
	private function getLastSyncTime($table_name, $bucket) {
		try {
			$result = $bucket->get($table_name . '_last_sync_time');
			if(isset($result->value->time)) {
				return $result->value->time;
			} else {
				$now = time();
				$bucket->replace($table_name . '_last_sync_time', ['time' => $now]);
				return $now;
			}
		} catch (\CouchbaseException $ex) {
			$now = time();
			$bucket->insert($table_name . '_last_sync_time', ['time' => $now]);
			return $now;
		}
	}

	public function exists($cb_obj, $key) {
    try{
    	$obj = $cb_obj->get($key);
    	if(empty($obj)) {
    		return false;
    	} else {
    		return true;
    	}
    } catch(\CouchbaseException $ex) {
    	return false;	
    }
	}
}