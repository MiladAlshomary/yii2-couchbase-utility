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
	* Takes sqlConnection component name ( see https://github.com/MiladAlshomary/couchbase-yii2), sqlConnection component name , table name of table to be imported
	* bucket_name is the name of bucket to export data to, fields is array of key/value to map fields into specific names
	**/
	public function actionImport($sqlConnection, $cbConnection, $table, $bucket_name, $fields = null, $offset = 0) {
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

		$query = new Query;

		if(empty($fields)) {
			$fields = '*';
		}

		$limit  = 1000;

		//load first batch
		$query->select($fields)->from($table)->limit($limit)->offset($offset);
		$rows = $query->all();
		while (!empty($rows)) {
			foreach ($rows as $document) {
				$offset ++;
				//push document into couchbase
				$document['cb_type'] = $table;
				$cb_bucket->insert( $table . '_' . $document['id'],$document);
				echo 'pushing document with offset : ' . $offset .PHP_EOL;
			}

			//load another batch
			$query->select($fields)->from($table)->limit($limit)->offset($offset);
			$rows = $query->all();
		}
		
		echo 'Ohhh life is bigger than you and you are not me ... Done :) '. PHP_EOL;
		
	}

	/**
	* Takes sqlConnection component name ( see https://github.com/MiladAlshomary/couchbase-yii2), sqlConnection component name , table name of table to be synched
	* bucket_name is the name of bucket to synch data to, fields is array of key/value to map fields into specific names
	**/
	public function actionSync($sqlConnection, $cbConnection, $table, $bucket_name, $sync_field, $fields = null) {
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

		//retrieve couchbase timestamp for the last sync
		$last_sync_date = $this->getLastSyncTime($table, $cb_bucket);	
		$query = new Query;

		if(empty($fields)) {
			$fields = '*';
		}

		$offset = 0;
		$limit  = 1000;

		//load first batch
		$query->select($fields)->from($table)->where($sync_field . ' > \'' . date('Y-m-d H:i:s',$last_sync_date) . '\'')->limit($limit)->offset($offset);
		$rows = $query->all();
		while (!empty($rows)) {
			foreach ($rows as $document) {
				$offset ++;
				//push document into couchbase
				$document['cb_type'] = $table;
				$cb_bucket->replace( $table . '_' . $document['id'],$document);
				echo 'pushing document with offset : ' . $offset .PHP_EOL;
			}

			//load another batch
			$query->select($fields)->from($table)->limit($limit)->offset($offset);
			$rows = $query->all();
		}
		
		//update the couchbase sync time stamp
		$cb_bucket->insert($table . '_last_sync_time', ['time' => time()]);
		
		echo 'Ohhh life is bigger than you and you are not me ... Done :) '. PHP_EOL;

	}

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
}