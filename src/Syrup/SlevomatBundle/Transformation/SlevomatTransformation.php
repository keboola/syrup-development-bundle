<?php
/**
 * Created by JetBrains PhpStorm.
 * User: Miro
 * Date: 10.12.12
 * Time: 10:37
 * To change this template use File | Settings | File Templates.
 */

namespace Syrup\SlevomatBundle\Transformation;

use \Syrup\ComponentBundle\Component\Component;

class SlevomatTransformation extends Component
{
	protected $_name = 'slevomat';
	protected $_prefix = 'rt';

	protected function _process($config, $params)
	{
		if (!isset($params['source']) || !isset($params['destination'])) {
			throw new \Exception("Source and destination parameters are required.");
		}

		$transformConfig = array();
		if (isset($config['configuration'])) {
			$transformConfig = $config['configuration'];
		}

		if (!isset($transformConfig['import']) || $transformConfig['import'] == 1) {
			$this->_log->info("Importing source - START");
			//$this->_import($params['source'], $transformConfig);
			$this->_log->info("Importing source - END");
		}

		if (!isset($transformConfig['transform']) || $transformConfig['transform'] == 1) {
			$this->_log->info("Transforming - START");
			$this->_transform();
			$this->_log->info("Transforming - END");
		}

		if (!isset($transformConfig['export']) || $transformConfig['export'] == 1) {
			$this->_log->info("Exporting output - START");
			$this->_export($params['destination']);
			$this->_log->info("Exporting output - END");
		}

	}

	protected function _import($sourceBucket, $transformConfig)
	{
		$tables = $this->_storageApi->listTables($sourceBucket);

		foreach($tables as $t) {

			$this->_log->info("Importing table " . $t['id']);

			$table = $this->_storageApi->getTable($t['id']);
			$tableName = $sourceBucket . "." . $table['name'];

			$sql = "DROP TABLE IF EXISTS `" . $tableName . "`";
			$this->_db->query($sql);

			$sql = "CREATE TABLE `" . $tableName . "` (";
			foreach ($table['columns'] as $col) {
				$sql .= "`" . $col . "` TEXT,";
			}
			$sql = substr($sql, 0, -1);
			$sql .= ")";
			$this->_db->query($sql);

			$filename = ROOT_PATH . "/tmp/" . $this->_prefix . '-' . $this->_name . '-' . $tableName . "_" . md5(microtime()) . ".csv";

			$this->_storageApi->exportTable($table['id'], $filename);

			$this->_db->query("
				LOAD DATA LOCAL INFILE '{$filename}' INTO TABLE {$table['name']}
                FIELDS TERMINATED BY ',' ENCLOSED BY '\"'
                LINES TERMINATED BY '\n'
                IGNORE 1 LINES"
			);

			unlink($filename);
		}
	}

	protected function _transform()
	{
		$this->_initTransform();

		$this->_processCreateProductSnapshots();

		$this->_processAssignDealCategories();

		$this->_processAssignDealLimitsToProducts();

		// Days since last orders snapshots
		$this->_processDaysSinceLastOrderSnapshots();

		// Transform sales targets
		$this->_processTransformSalesTargets();

		// Assigning salesman to products
		$this->_processAssignSalesmenToProducts();

		$this->_processUpdateCancelledVouchers();

		// Salesman Bonuses
		$this->_processComputeSalaries();

		// Newsletters to Orders
		$this->_processNewslettersToOrders();

		// Manager and ManagerTargets
		$this->_processAssignManagers();
	}

	protected function _export($destinationBucket)
	{
		$dbConfig = $this->_db->getConfiguration();
		//var_dump($dbConfig); die;

		if (!$this->_storageApi->bucketExists($destinationBucket)) {
			$destBucketArr = explode('.', $destinationBucket);
			$this->_storageApi->createBucket($destBucketArr[1], $destBucketArr[0], 'Remote Transformation Output');
		}
		// get tables from Slevomat_out
		$outTables = $this->_db->query("SHOW TABLES");
		foreach($outTables as $t) {

			$outTableName = array_shift($t);
			$outFilename = tempnam(ROOT_PATH . "/tmp", $outTableName) . ".csv";
			$errorFilename = tempnam(ROOT_PATH . "/tmp", $outTableName) . ".csv.error";

			$select = "SELECT * FROM `{$outTableName}`;";

			$command = 'mysql -u ' . $dbConfig->user
				. ' -p' . $dbConfig->password
				. ' -h ' . $dbConfig->host
				. ' '. $dbConfig->dbname . ' -B -e ' . escapeshellarg((string) $select)
				. ' --quick 2> ' . $errorFilename;

			$conversionPath = ROOT_PATH . "/library/Keboola/Db/CsvExport/conversion.php";
			$command .= ' | php ' . $conversionPath . " csv";
			$command .= ' > ' . $outFilename;

			//$log->log("{$logMessageIn}: PROGRESS", \Zend_Log::INFO, array_merge($logDataIn, array("progress" => "Ready to export data from Transformation DB")));

			$result = exec($command);

			if ($result != "" || file_exists($errorFilename) && filesize($errorFilename) > 0) {
				$error = $result;
				if ($error == '') {
					$error = trim(file_get_contents($errorFilename));
				}
				throw new Exception("MySQL export error: " . $error);
			}
			//$log->log("{$logMessageIn}: PROGRESS", \Zend_Log::INFO, array_merge($logDataIn, array("progress" => "Data from Transformation DB exported")));
			//$log->log("{$logMessageIn}: PROGRESS", \Zend_Log::INFO, array_merge($logDataIn, array("progress" => "Uploading data to Storage API")));

			if (!$this->_storageApi->tableExists($destinationBucket . '.' . $outTableName)) {
				$this->_storageApi->createTable($destinationBucket, $outTableName, $outFilename);
			} else {
				$this->_storageApi->writeTable($destinationBucket . '.' . $outTableName, $outFilename);
			}
		}
	}

	protected function _initTransform()
	{
		$this->_db->query("
			ALTER TABLE `out.users` ADD INDEX `id` (`id`(11));
		");
		$this->_db->query("
			ALTER TABLE `out.carts`
			ADD INDEX `id` (`id`(11)),
			ADD INDEX `user` (`user`(11));
		");
		$this->_db->query("
			ALTER TABLE `out.marginLimits`
			ADD INDEX `id` (`id`(11)),
			ADD INDEX `salesman` (`salesman`(11));
		");
		$this->_db->query("
			ALTER TABLE `out.newsletters`
			ADD INDEX 	`deleted` (`deleted`(11));
		");
		$this->_db->query("
			ALTER TABLE `out.orders`
			ADD INDEX `id` (`id`(11)),
			ADD INDEX `user` (`user`(11)),
			ADD INDEX `product` (`product`(11)),
			ADD INDEX `variant` (`variant`(11)),
			ADD INDEX `dateTime` (`dateTime`(11)),
			ADD INDEX `user_id` (`user`(11),`id`(11)),
			ADD INDEX `id_user` (`id`(11),`user`(11)),
			ADD INDEX `paid` (`paid`(11)),
			ADD INDEX `cart` (`cart`(11));
		");
		$this->_db->query("
			ALTER TABLE `out.products`
			ADD INDEX `id` (`id`(11)),
			ADD INDEX `partner` (`partner`(11));
		");
		$this->_db->query("
			ALTER TABLE `out.ratings`
			ADD INDEX `voucher` (`voucher`(11)),
			ADD INDEX `user` (`user`(11));
		");
		$this->_db->query("
			ALTER TABLE `out.variants`
			ADD INDEX `id` (`id`(11)),
			ADD INDEX `product` (`product`(11));
		");
		$this->_db->query("
			ALTER TABLE `out.user_ratings`
			ADD INDEX `partner` (`partner`(11)),
			ADD INDEX `product` (`product`(11)),
			ADD INDEX `voucher` (`voucher`(11)),
			ADD INDEX `user` (`user`(11));
		");
		$this->_db->query("
			ALTER TABLE `out.vouchers`
			ADD INDEX `id` (`id`(11)),
			ADD INDEX `order` (`order`(11));
		");
		$this->_db->query("
			ALTER TABLE `out.daysSinceLastOrderSnapshot`
			ADD INDEX `id` (`id`(11));
		");
		$this->_db->query("
			ALTER TABLE `out.partners`
			ADD INDEX `id` (`id`(11));
		");
	}

	/**
	 *
	 * create product snapshots
	 * @return bool
	 */
	protected function _processCreateProductSnapshots()
	{
		$db = $this->_db;
		// calculate additional data
		print "Creating products-date snapshots... ";
		//NDebugger::timer("productsSnapshots");
		$db->query("DROP TABLE IF EXISTS `out.productsSnapshots`;");
		$db->query("CREATE TABLE `out.productsSnapshots` (
                `id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
                `date` date NOT NULL,
                `product` int unsigned NOT NULL,
                `price` decimal(16,2) NOT NULL DEFAULT '0',
                `commission` decimal(16,2) NOT NULL DEFAULT '0'
			) COMMENT='' ENGINE='InnoDB' COLLATE 'utf8_general_ci';");

		$products = $db->fetchAssoc("SELECT id, start, end, price, commission FROM products");

		foreach($products as $product) {
			$dates = array();
			$dateFrom = date("Y-m-d", strtotime($product["start"]));
			// Some dates end 2012-04-18 23:59:59, some 2012-04-20 00:00:00, so we subtract 60 seconds to be sure
			$dateTo = date("Y-m-d", strtotime($product["end"]) - 60);
			$dates[] = $dateFrom;
			$currentDate = $dateFrom;
			while($currentDate < $dateTo) {
				$currentDate = date("Y-m-d", strtotime("+1 day", strtotime($currentDate)));
				$dates[] = $currentDate;
			}
			// Insert into DB
			$query = "INSERT INTO `out.productsSnapshots` (`date`, product, price, commission) VALUES ";
			$queryArr = array();
			foreach ($dates as $date) {
				$queryArr[] = "('{$date}', {$product["id"]}, '{$product["price"]}', '{$product["commission"]}')";
			}
			$db->query($query . join(", ", $queryArr));

		}
		$this->log->info("Slevomat: Created products-date snapshots.");
		print "OK\n";
		return true;
	}

	/**
	 *
	 * Assign deal categories to products
	 *
	 * @param $db
	 * @return bool
	 */
	protected function _processAssignDealCategories() {
		$db = $this->_db;
		print "Computing deal categories... ";
		//NDebugger::timer("productsDealCategories");
		$products = $db->fetchAll("SELECT id, cities FROM products");
		foreach ($products as $product) {
			if ($product["cities"] == "Praha") {
				$db->query("UPDATE `out.products` SET dealCategory = 'Praha', dealCategorySort = 1 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Praha Extra") {
				$db->query("UPDATE `out.products` SET dealCategory = 'Praha Extra', dealCategorySort = 2 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Brno") {
				$db->query("UPDATE `out.products` SET dealCategory = 'Brno', dealCategorySort = 3 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Ostrava") {
				$db->query("UPDATE `out.products` SET dealCategory = 'Ostrava', dealCategorySort = 4 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Olomouc a Haná") {
				$db->query("UPDATE `out.products` SET dealCategory = 'Olomouc a Haná', dealCategorySort = 5 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "HK a Pardubice") {
				$db->query("UPDATE `out.products` SET dealCategory = 'HK a Pardubice', dealCategorySort = 6 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Zlín a okolí") {
				$db->query("UPDATE `out.products` SET dealCategory = 'Zlín a okolí', dealCategorySort = 7 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Plzeň") {
				$db->query("UPDATE `out.products` SET dealCategory = 'Plzeň', dealCategorySort = 8 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Liberec") {
				$db->query("UPDATE `out.products` SET dealCategory = 'Liberec', dealCategorySort = 9 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Karlovy Vary") {
				$db->query("UPDATE `out.products` SET dealCategory = 'Karlovy Vary', dealCategorySort = 10 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "České Budějovice") {
				$db->query("UPDATE `out.products` SET dealCategory = 'České Budějovice', dealCategorySort = 11 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Ústí n. L. a Teplice") {
				$db->query("UPDATE `out.products` SET dealCategory = 'Ústí n. L. a Teplice', dealCategorySort = 12 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Jihlava a Vysočina") {
				$db->query("UPDATE `out.products` SET dealCategory = 'Jihlava a Vysočina', dealCategorySort = 13 WHERE id = {$product["id"]}");
				continue;
			}
			if (strpos($product["cities"], "Móda") !== false) {
				$db->query("UPDATE `out.products` SET dealCategory = 'Móda', dealCategorySort = 14 WHERE id = {$product["id"]}");
				continue;
			}
			if (strpos($product["cities"], "Cestování") !== false) {
				$db->query("UPDATE `out.products` SET dealCategory = 'Cestování', dealCategorySort = 15 WHERE id = {$product["id"]}");
				continue;
			}
			if (strpos($product["cities"], "Zboží") !== false) {
				$db->query("UPDATE `out.products` SET dealCategory = 'Zboží', dealCategorySort = 16 WHERE id = {$product["id"]}");
				continue;
			}
			if (strpos($product["cities"], "Víno") !== false) {
				$db->query("UPDATE `out.products` SET dealCategory = 'Víno', dealCategorySort = 17 WHERE id = {$product["id"]}");
				continue;
			}
			if (strpos($product["cities"], "Praha") !== false || strpos($product["cities"], "Praha Extra") !== false) {
				$db->query("UPDATE `out.products` SET dealCategory = 'Pha', dealCategorySort = 18 WHERE id = {$product["id"]}");
				continue;
			}
			if (strpos($product["cities"], "Brno") !== false
				|| strpos($product["cities"], "Ostrava") !== false
				|| strpos($product["cities"], "Plzeň") !== false
			)
			{
				$db->query("UPDATE `out.products` SET dealCategory = 'R1', dealCategorySort = 19 WHERE id = {$product["id"]}");
				continue;
			}
			if (strpos($product["cities"], "Olomouc a Haná") !== false
				|| strpos($product["cities"], "HK a Pardubice") !== false
				|| strpos($product["cities"], "Zlín a okolí") !== false
				|| strpos($product["cities"], "Liberec") !== false
				|| strpos($product["cities"], "Karlovy Vary") !== false
				|| strpos($product["cities"], "České Budějovice") !== false
				|| strpos($product["cities"], "Ústí n. L. a Teplice") !== false
				|| strpos($product["cities"], "Jihlava a Vysočina") !== false
			)
			{
				$db->query("UPDATE `out.products` SET dealCategory = 'R2', dealCategorySort = 20 WHERE id = {$product["id"]}");
				continue;
			}
			$db->query("UPDATE `out.products` SET dealCategory = 'Other', dealCategorySort = 21 WHERE id = {$product["id"]}");
		}
		$this->log->info("Slevomat: Assigned deal categories.");
		print "OK\n";
		return true;
	}

	/**
	 *
	 * Assign deal limits to products
	 *
	 * @param $db
	 * @return bool
	 */
	protected function _processAssignDealLimitsToProducts() {
		print "Assigning deal limits to products... ";
		//NDebugger::timer("productsDealLimits");

		$db = $this->_db;

		$db->query("TRUNCATE productsDealLimits;");
		$limits = $db->fetchAssoc("SELECT id, name, search FROM `out.dealLimits` ORDER BY sort");
		foreach($limits as $limit) {
			$db->query("
				INSERT INTO `out.productsDealLimits`
				SELECT products.id, '{$limit["id"]}' FROM `out.products` products
				LEFT JOIN `out.productsDealLimits` ON products.id = `out.productsDealLimits`.product
				WHERE `out.productsDealLimits`.product IS NULL AND dealCategory = '{$limit["search"]}'
			");
		}
		$this->log->info("Slevomat: Assigned deal limits.");
		print "OK\n";
		return true;
	}

	/**
	 *
	 * Compute days since last order snapshots for each user
	 *
	 * @param $db
	 * @return bool
	 */
	protected function _processDaysSinceLastOrderSnapshots() {
		print "Computing days since last order snapshots... ";
		//NDebugger::timer("daysSinceLastOrderSnapshots");
		// DO NOT TRUNCATE, heavy processing when updating all records
		// $db->query("TRUNCATE daysSinceLastOrderSnapshots;");

		$db = $this->_db;

		$dates = array();
		$dateFrom = "2010-04-11";
		$dateTo = date("Y-m-d", strtotime("yesterday"));
		$dates[] = $dateFrom;
		$currentDate = $dateFrom;
		while($currentDate < $dateTo) {
			$currentDate = date("Y-m-d", strtotime("+1 day", strtotime($currentDate)));
			$dates[] = $currentDate;
		}
		$insertedDates = $db->fetchAll("SELECT DISTINCT snapshotDate FROM `out.daysSinceLastOrderSnapshot`");
		$datesToInsert = array_diff ($dates, (array_map(function($array) {return $array["snapshotDate"];}, $insertedDates)));

		foreach($datesToInsert as $date) {

			//NDebugger::timer("daysSinceLastOrderSnapshotsDay");
			$query = "
				SELECT COUNT(id) AS pocet, daysSinceLastOrderCat
				FROM (
				SELECT t.id,
				CASE
					WHEN MAX(o.dateTime) IS NULL || MAX(o.dateTime) = '0000-00-00 00:00:00' THEN '--ještě neobjednal--'
					WHEN DATEDIFF('{$date}', MAX(o.dateTime)) > 360 THEN '> 12 měsíců'
					WHEN DATEDIFF('{$date}', MAX(o.dateTime)) > 180 THEN '6-12 měsíců'
					WHEN DATEDIFF('{$date}', MAX(o.dateTime)) > 90 THEN '3-6 měsíců'
					ELSE '0-3 měsíce'
				END
				 AS daysSinceLastOrderCat
				FROM `out.users` t
				LEFT JOIN `out.orders` o ON o.user = t.id AND o.dateTime < '{$date}'
				WHERE t.regtime < '{$date}'
				GROUP BY t.id
				) tmp
				GROUP BY daysSinceLastOrderCat;
			";
			$result = $db->fetchAll($query);
			$data = array(
				"--ještě neobjednal--" => 0,
				"> 12 měsíců" => 0,
				"6-12 měsíců" => 0,
				"3-6 měsíců" => 0,
				"0-3 měsíce" => 0
			);

			foreach($result as $row) {
				$data[$row["daysSinceLastOrderCat"]] = $row["pocet"];
			}

			$db->query("
				INSERT INTO `out.daysSinceLastOrderSnapshot` (snapshotDate, category, categorySort, `count`)
				VALUES
				('{$date}', '--ještě neobjednal--', 4, '{$data['--ještě neobjednal--']}'),
				('{$date}', '> 12 měsíců', 3, '{$data['> 12 měsíců']}'),
				('{$date}', '6-12 měsíců', 2, '{$data['6-12 měsíců']}'),
				('{$date}', '3-6 měsíců', 1, '{$data['3-6 měsíců']}'),
				('{$date}', '0-3 měsíce', 0, '{$data['0-3 měsíce']})');");

			$this->log->log("Slevomat: Computed daysSinceLastOrder snapshot for {$date}.");

		}
		$this->log->info("Slevomat: Computed daysSinceLastOrder snapshots.");
		print "OK\n";
		return true;
	}

	/**
	 *
	 * Assign salesmen to products
	 *
	 * @param $db
	 * @return bool
	 */
	protected function _processAssignSalesmenToProducts()
	{
		$db = $this->_db;

		print "Assigning salesman to products... ";
//		NDebugger::timer("salesmanToProducts");

		$salesmen = $db->fetchAll("SELECT id, name FROM `out.salesman` WHERE name != '--empty--'");
		$emptySalesman = $db->fetchRow("SELECT id, name FROM `out.salesman` WHERE name = '--empty--'");

		$salesmanNames = array();
		foreach ($salesmen as $salesman) {
			$db->query("UPDATE `out.products` SET salesmanId = {$salesman["id"]} WHERE salesman = '{$salesman["name"]}'");
			$salesmanNames[] = "'{$salesman["name"]}'";
		}
		$db->query("UPDATE `out.products` SET salesmanId = {$emptySalesman["id"]} WHERE salesman NOT IN (" . join(",", $salesmanNames) . ")");

		$this->log->info("Slevomat: Assigned salesman to products.");
		print "OK\n";
		return true;

	}


	/**
	 *
	 * Compute salesmen salaries
	 *
	 * @param $db
	 * @return bool
	 */
	protected function _processComputeSalaries()
	{
		$db = $this->_db;
		// Computing salesman salaries
		print "Computing salesmen salaries... ";
		//NDebugger::timer("salesmanSalaries");

		// Truncate table salesmanBonus
		$db->query("TRUNCATE TABLE salesmanBonus");

		// All missing dates
		$lastDate = $db->fetchColumn("SELECT IFNULL(MAX(`date`), '2012-04-01') FROM `out.salesmanBonus`;");
		$dateTo = date("Y-m-d");
		$currentDate = $lastDate;
		if ($lastDate != $dateTo) {
			while($currentDate <= $dateTo) {
				// H1, H2, H3, H4, H6
				$this->_computeSalaries($currentDate);
				// H5
				$this->_computeH5($currentDate);

				$currentDate = date("Y-m-d", strtotime("+1 day", strtotime($currentDate)));
			}
		}

		// Targets
		$this->_computeTargetFulfillments($db);

		$this->log->info("Slevomat: Computed salesmen salaries.");
		print "OK\n";

		return true;
	}

	/**
	 *
	 * Internal routine to calculate salaries for a specific day
	 *
	 * @param Zend_Db_Adapter_Abstract $db
	 * @param $currentDate string
	 */
	private function _computeSalaries($currentDate)
	{
		$db = $this->_db;

		$salaryMonths =  array(date("Y-m", strtotime($currentDate)));
		if (date("d", strtotime($currentDate)) <= 13) {
			$salaryMonths[] = date("Y-m", strtotime("-1 month", strtotime($currentDate)));
		}

		$salesmen = $db->fetchAll("SELECT id, name, region FROM `out.salesman` WHERE name != '--empty--'");

		foreach ($salesmen as $salesman) {

			foreach($salaryMonths as $salaryMonth) {
				// print "Salesman {$salesman["name"]}, region {$salesman["region"]}, month {$salaryMonth}\n";
				$bonuses = array(
					"H1" => 0,
					"H1count" => 0,
					"H2" => 0,
					"H3" => 0,
					"H4" => 0,
					"H5" => 0,
					"H6" => 0,
					"total" => 0,
					"citiesMargin" => 0,
					"tabsMargin" => 0

				);
				// H1 bonus
				switch($salesman["region"]) {
					case "R1":
					case "R2":
						$query = "
							SELECT
								products.id,
								SUM(orders.commission/1.20),
								dealLimits.limitMinimum,
								SUM(orders.commission/1.20) / SUM(orders.totalPrice),
								products.noShows,
								orders.totalPrice
							FROM `out.orders` orders
							LEFT JOIN `out.products` products ON orders.product = products.id
							LEFT JOIN `out.productsDealLimits` productsDealLimits ON products.id = productsDealLimits.product
							LEFT JOIN `out.dealLimits` dealLimits ON productsDealLimits.dealLimit = dealLimits.id
							WHERE `out.products`.salesmanId = {$salesman["id"]}
								AND products.start >= '{$salaryMonth}-01'
								AND products.start <= '{$salaryMonth}-31'
								AND orders.dateTime <= '{$currentDate}'
								AND orders.paid = 'Yes'
     							AND orders.cancelledVoucher = 'No'
     							AND orders.cancelled = 'No'
							GROUP BY products.id
							HAVING
								SUM(orders.totalPrice) >= dealLimits.limitMinimum
          						AND (SUM(orders.commission/1.20) / SUM(orders.totalPrice) >= 0.22
          							OR products.noShows >= 50)
								";
						$deals = $db->fetchAll($query);
						// print "H1 - " . count($deals) . " deals\n";
						$bonuses["H1"] = count($deals) * 1000;
						$bonuses["H1count"] = count($deals);
						break;
					case "P":
						$query = "
							SELECT
								products.id,
								SUM(orders.commission/1.20),
								SUM(orders.commission/1.20) / SUM(orders.totalPrice),
								orders.totalPrice,
								products.noShows,
								dealLimits.limitMinimum
							FROM `out.orders` orders
							LEFT JOIN `out.products` products ON orders.product = products.id
							LEFT JOIN `out.productsDealLimits` productsDealLimits ON productsDealLimits.product = products.id
							LEFT JOIN `out.dealLimits` dealLimits ON productsDealLimits.dealLimit = dealLimits.id
							WHERE products.salesmanId = {$salesman["id"]}
								AND products.start >= '{$salaryMonth}-01'
								AND products.start <= '{$salaryMonth}-31'
								AND orders.dateTime <= '{$currentDate}'
								AND orders.paid = 'Yes'
     							AND orders.cancelledVoucher = 'No'
     							AND orders.cancelled = 'No'
							GROUP BY products.id
							HAVING
								SUM(orders.totalPrice) >= dealLimits.limitMinimum
          						AND SUM(orders.commission/1.20) / SUM(orders.totalPrice) >= 0.18
         						AND products.noShows >= 50
         						AND products.noShows < 100
							";
						$deals = $db->fetchAll($query);
						$bonuses["H1"] += count($deals) * 500;
						$bonuses["H1count"] += count($deals);
						// print "H1 - " . count($deals) . " (a) deals\n";

						$query = "
							SELECT
								products.id,
								SUM(orders.commission),
								SUM(orders.commission) / SUM(orders.totalPrice),
								orders.totalPrice,
								products.noShows,
								dealLimits.limitMinimum
							FROM `out.orders` orders
							LEFT JOIN `out.products` products ON orders.product = products.id
							LEFT JOIN `out.productsDealLimits` productsDealLimits ON productsDealLimits.product = products.id
							LEFT JOIN `out.dealLimits` dealLimits ON productsDealLimits.dealLimit = dealLimits.id
							WHERE products.salesmanId = {$salesman["id"]}
								AND products.start >= '{$salaryMonth}-01'
								AND products.start <= '{$salaryMonth}-31'
								AND orders.dateTime <= '{$currentDate}'
								AND orders.paid = 'Yes'
     							AND orders.cancelledVoucher = 'No'
     							AND orders.cancelled = 'No'
							GROUP BY products.id
							HAVING
								SUM(orders.totalPrice) >= dealLimits.limitMinimum
								AND products.noShows = 100
							";
						$deals = $db->fetchAll($query);
						// print "H1 - " . count($deals) . " (b) deals\n";
						$bonuses["H1"] += count($deals) * 1000;
						$bonuses["H1count"] += count($deals);

						break;
				}

				// H2 bonus
				$tabbedProducts = $db->fetchColumn("
					SELECT IF(SUM(orders.commission) IS NULL, 0, SUM(orders.commission)) / 1.2 AS grossMargin
					FROM `out.orders` orders LEFT JOIN `out.products` products ON orders.product = products.id
					WHERE products.salesmanId = {$salesman["id"]}
						AND products.dealCategory IN ('Cestování', 'Zboží')
						AND products.start >= '{$salaryMonth}-01'
						AND products.start <= '{$salaryMonth}-31'
						AND orders.paid = 'Yes'
						AND orders.cancelledVoucher = 'No'
						AND orders.cancelled = 'No'
						AND orders.dateTime <= '{$currentDate}'
					");
				$normalProducts = $db->fetchColumn("
					SELECT IF(SUM(orders.commission) IS NULL, 0, SUM(orders.commission)) / 1.2 AS grossMargin
					FROM `out.orders` orders LEFT JOIN `out.products` products ON orders.product = products.id
					WHERE products.salesmanId = {$salesman["id"]}
						AND products.dealCategory NOT IN ('Cestování', 'Zboží')
						AND products.start >= '{$salaryMonth}-01'
						AND products.start <= '{$salaryMonth}-31'
						AND orders.paid = 'Yes'
						AND orders.cancelledVoucher = 'No'
						AND orders.cancelled = 'No'
						AND orders.dateTime <= '{$currentDate}'
					");

				$bonuses["tabsMargin"] = $tabbedProducts;
				$bonuses["citiesMargin"] = $normalProducts;

				switch($salesman["region"]) {
					case "R1":
						$bonuses["H2"] += ($tabbedProducts + $normalProducts) * 0.1;
						break;
					case "R2":
						$bonuses["H2"] += ($tabbedProducts + $normalProducts) * 0.12;
						break;
					case "P":
						if ($normalProducts < 400000) {
							$bonuses["H2"] += $normalProducts * 0.085 + $tabbedProducts * 0.08;
						} elseif ($normalProducts < 500000) {
							$bonuses["H2"] += $normalProducts * 0.09 + $tabbedProducts * 0.09;
						} elseif ($normalProducts < 600000) {
							$bonuses["H2"] += $normalProducts * 0.095 + $tabbedProducts * 0.09;
						} elseif ($normalProducts < 700000) {
							$bonuses["H2"] += $normalProducts * 0.1 + $tabbedProducts * 0.09;
						} elseif ($normalProducts < 800000) {
							$bonuses["H2"] += $normalProducts * 0.105 + $tabbedProducts * 0.09;
						} elseif ($normalProducts < 900000) {
							$bonuses["H2"] += $normalProducts * 0.11 + $tabbedProducts * 0.09;
						} elseif ($normalProducts < 1000000) {
							$bonuses["H2"] += $normalProducts * 0.115 + $tabbedProducts * 0.09;
						} else {
							$bonuses["H2"] += $normalProducts * 0.12 + $tabbedProducts * 0.1;
						}
						break;
				}

				// H3 bonus
				$personalLimit = $db->fetchArray("SELECT marginLimit, bonus FROM `out.marginLimits` WHERE date = '{$salaryMonth}-01}' AND salesman = {$salesman["id"]}");

				if ($personalLimit["marginLimit"] <= ($tabbedProducts + $normalProducts)) {
					$bonuses["H3"] += $personalLimit["bonus"];
				} elseif ($personalLimit["marginLimit"] * 0.75 <= ($tabbedProducts + $normalProducts)) {
					$bonuses["H3"] += $personalLimit["bonus"] * 0.5;
				}

				// H4 bonus
				$phonies = $db->fetchColumn("SELECT salesman FROM `out.phones`");

				// No bonus for phonies :P
				if (!in_array($salesman['name'], $phonies)) {
					$bonuses["H4"] += 2000;
				}

				// H6 bonus
				switch($salesman["region"]) {
					case "R1":
					case "R2":
						$query = "
							SELECT
								products.id,
								SUM(orders.commission/1.20),
								dealLimits.limitMinimum,
								SUM(orders.commission/1.20) / SUM(orders.totalPrice),
								products.noShows,
								orders.totalPrice
							FROM `out.orders` orders
							LEFT JOIN `out.products` products ON orders.product = products.id
							LEFT JOIN `out.productsDealLimits` productsDealLimits ON products.id = productsDealLimits.product
							LEFT JOIN `out.dealLimits` dealLimits ON productsDealLimits.dealLimit = dealLimits.id
							WHERE products.salesmanId = {$salesman["id"]}
								AND products.start >= '{$salaryMonth}-01'
								AND products.start <= '{$salaryMonth}-31'
								AND orders.dateTime <= '{$currentDate}'
								AND orders.paid = 'Yes'
     							AND orders.cancelledVoucher = 'No'
     							AND orders.cancelled = 'No'
							GROUP BY products.id
							HAVING
								SUM(orders.totalPrice) >= dealLimits.limitMinimum
          						AND (SUM(orders.commission/1.20) / SUM(orders.totalPrice) >= 0.18
          						AND products.noShows = 100)
						";
						$deals = $db->fetchAll($query);
						// print "H6 - " . count($deals) . " deals\n";
						$bonuses["H6"] = count($deals) * 500;
						$bonuses["H6count"] = count($deals);
						break;
				}

				// print "Bonuses: \n";
				$bonuses["total"] = $bonuses["H1"] + $bonuses["H2"] + $bonuses["H3"] + $bonuses["H4"] + $bonuses["H6"];

				// Update db
				$row = $db->fetchArray("SELECT * FROM `out.salesmanBonus` WHERE salesman = {$salesman["id"]} AND `date` = '{$currentDate}' AND salaryDate = '{$salaryMonth}-01'");
				if (!$row) {
					$db->query("
						INSERT INTO `out.salesmanBonus` SET
							salesman = '{$salesman["id"]}',
							`date` = '{$currentDate}',
							salaryDate = '{$salaryMonth}-01',
							h1 = {$bonuses["H1"]},
							h1count = {$bonuses["H1count"]},
							h2 = {$bonuses["H2"]},
							h3 = {$bonuses["H3"]},
							h4 = {$bonuses["H4"]},
							h5 = 0,
							h6 = {$bonuses["H6"]},
							total = {$bonuses["total"]},
							citiesMargin = {$bonuses["citiesMargin"]},
							tabsMargin = {$bonuses["tabsMargin"]}

						");
				} else {
					$db->query("
						UPDATE `out.salesmanBonus` SET
							h1 = {$bonuses["H1"]},
							h1count = {$bonuses["H1count"]},
							h2 = {$bonuses["H2"]},
							h3 = {$bonuses["H3"]},
							h4 = {$bonuses["H4"]},
							h6 = {$bonuses["H6"]},
							total = {$bonuses["total"]},
							citiesMargin = {$bonuses["citiesMargin"]},
							tabsMargin = {$bonuses["tabsMargin"]}
						WHERE id = {$row["id"]}
					");
				}
			}
		}
	}

	/**
	 *
	 * @param Zend_Db_Adapter_Abstract $db
	 */
	protected function _computeH5($currentDate)
	{
		$db = $this->_db;

		$salesmanBonuses = $db->fetchAll("
			SELECT sb.id, sb.salesman, sb.date, sb.salaryDate, s.team, sb.citiesMargin + sb.tabsMargin AS salesmanCommission FROM `out.salesmanBonus` sb
			LEFT JOIN salesman s ON (sb.salesman = s.id)
			WHERE (sb.citiesMargin != 0 OR sb.tabsMargin != 0) AND `date` = '{$currentDate}' AND team != 1000
		");

		foreach ($salesmanBonuses as $salesmanBonusRow) {

			$teamTarget = $db->fetchColumn("SELECT value FROM `out.teamTargets` WHERE `key` = 'Marze bez DPH' AND month = '" . $salesmanBonusRow['salaryDate'] . "' AND team='" . $salesmanBonusRow['team'] . "'");
			$teamBonus = $db->fetchColumn("SELECT value FROM `out.teamTargets` WHERE `key` = 'Bonus' AND month = '" . $salesmanBonusRow['salaryDate'] . "' AND team='" . $salesmanBonusRow['team'] . "'");

			$teamCommission = $db->fetchColumn("
				SELECT SUM(m.teamMargin) AS teamCommission FROM (
					SELECT MAX(sb.citiesMargin) + MAX(sb.tabsMargin) AS teamMargin FROM `out.salesmanBonus` sb
					LEFT JOIN `out.salesman` s ON (sb.salesman = s.id)
					WHERE s.team = '" .$salesmanBonusRow['team'] . "'
					AND sb.salaryDate = '" . $salesmanBonusRow['salaryDate'] . "'
					GROUP BY sb.salesman
				) m
			");

			$salesmanCommission = $salesmanBonusRow['salesmanCommission'];

			if ($teamCommission >= $teamTarget) {
				$bonus = $teamBonus * ($salesmanCommission / $teamCommission);
				$db->query("
					UPDATE `out.salesmanBonus`
					SET h5 = " . $bonus . ",
					total = total + " . $bonus . "
					WHERE id='" . $salesmanBonusRow['id'] . "'
				");
			} else if ($teamCommission >= ($teamTarget * 0.75) ) {
				$bonus = $teamBonus * $salesmanCommission / $teamCommission * 0.5;
				$db->query("
					UPDATE `out.salesmanBonus`
					SET h5 = " . $bonus . ",
					total = total + " . $bonus . "
					WHERE id='" . $salesmanBonusRow['id'] . "'
				");
			}
		}
	}

	/**
	 *
	 * @param Zend_Db_Adapter_Abstract $db
	 * @return boolean
	 */
	protected function _computeTargetFulfillments()
	{
		$db = $this->_db;

		$db->query("
			UPDATE `out.salesmanBonus` SET target = (
				SELECT tf.value FROM `out.targetFulfillments` tf WHERE tf.id=DAY(salesmanBonus.date)
			) WHERE MONTH(date)=MONTH(salaryDate)
		");
		$db->query("
			UPDATE `out.salesmanBonus` SET target = (
				SELECT tf.value FROM `out.targetFulfillments` tf WHERE tf.id=(DAY(salesmanBonus.date) + 31)
			) WHERE MONTH(date)!=MONTH(salaryDate)
		");
		// if (date - salaryDate > 31 + 12) target = 100%
		$db->query("
			UPDATE `out.salesmanBonus` SET target = 1.00
			WHERE MONTH(date)!=MONTH(salaryDate) AND DAY(date) > 12
		");
	}

	/**
	 *
	 * Transform sales targets
	 * - add new salesmen
	 * - set margin limits
	 *
	 * @param $db
	 * @return bool
	 */
	protected function _processTransformSalesTargets()
	{
		$db = $this->_db;
		// Sales targets
		print "Transforming sales targets... ";
		//NDebugger::timer("salesTargets");

		// Create all salesman
		$db->query("TRUNCATE `out.salesman`;");
		$db->query("
			INSERT INTO `out.salesman`
			SELECT DISTINCT NULL, st.salesPerson, st.manager, st.region, IF (tm.id IS NULL, 1000, tm.id) AS team FROM `out.salesTargets` st
			LEFT JOIN `out.salesman` s ON s.name = st.salesPerson
			LEFT JOIN `out.teams` tm ON st.team = tm.name
			WHERE s.id IS NULL;
		");
		$db->query("INSERT INTO `out.salesman` SET id = 1000, name = '--empty--', manager = '--empty--', region = '---', team = 1000;");

		// Get the limits and insert into marginLimits
		$db->query("TRUNCATE `out.marginLimits`;");
		$salesmen = $db->fetchAll("SELECT id, name FROM `out.salesman` WHERE id != 1000");
		foreach ($salesmen as $salesman) {
			$dates = $db->fetchAll("SELECT DISTINCT `date` FROM `out.salesTargets` WHERE salesPerson = '{$salesman["name"]}';");
			foreach ($dates as $date) {
				$limit = $db->fetchColumn("SELECT value FROM `out.salesTargets` WHERE salesPerson = '{$salesman["name"]}' AND `date` = '{$date["date"]}' AND `key` = 'Marze bez DPH'");
				$bonus = $db->fetchColumn("SELECT value FROM `out.salesTargets` WHERE salesPerson = '{$salesman["name"]}' AND `date` = '{$date["date"]}' AND `key` = 'Bonus'");
				$db->query("INSERT INTO `out.marginLimits` SET salesman = '{$salesman["id"]}', `date` = '{$date["date"]}', marginLimit = '{$limit}', bonus = '{$bonus}'");
			}
		}

		$this->log->info("Slevomat: Transformed sales targets.");
		print "OK\n";
		return true;
	}


	/**
	 *
	 * Flags orders that have a cancelled vouchers
	 *
	 * @param $db
	 * @return bool
	 */
	protected function _processUpdateCancelledVouchers()
	{
		$db = $this->_db;

		// Sales targets
		print "Updating cancelled vouchers... ";
//		NDebugger::timer("cancelledVouchers");

		// Update all orders with cancelled vouchers
		$db->query("UPDATE `out.orders` SET cancelledVoucher = 'Yes' WHERE id IN(SELECT DISTINCT `out.vouchers`.order FROM `out.vouchers` WHERE cancelled = 'Yes')");

		$this->log->info("Slevomat: Updated cancelled vouhcers.");
		print "OK\n";
		return true;
	}

	protected function _processNewslettersToOrders()
	{
		$db = $this->_db;
		print "Newsletters to orders correlation... ";
//		NDebugger::timer("newslettersOrders");

		// Update all orders where user is subscribed to some newsletter
		$db->query("UPDATE `out.orders` SET newsletter = 1 WHERE `user` IN (SELECT DISTINCT `out.newsletters`.user FROM `out.newsletters` WHERE deleted = 'No')");

		$this->log->info("Slevomat: Updated orders with newsletter flag.");
		print "OK\n";
		return true;
	}

	protected function _processCityTargets()
	{
		$db = $this->_db;
		print "Assign City Targets to Orders... ";
//		NDebugger::timer("citytargets");

		$db->query("
			UPDATE `out.orders` o SET city = (
				SELECT ct.id FROM `out.cityTargets` ct WHERE ct.city = o.city
			)
		");

		$this->log->info("Slevomat: Updated orders with newsletter flag.");
		print "OK\n";
		return true;
	}

	protected function _processAssignManagers()
	{
		$db = $this->_db;

		print "Creating managers table... ";
//		NDebugger::timer("team");


		$db->query("TRUNCATE `out.managers`");
		$db->query("
			INSERT INTO `out.managers` (name)
			SELECT DISTINCT manager FROM `out.salesman` WHERE manager != '--empty--'
		");
		$db->query("
			REPLACE INTO `out.managers`
			SET id = 1000, name = '--empty--'
		");


		$this->log->info("Slevomat: Created teams tables.");
		print "OK\n";

		print "Assign Managers to Salesman and Targets... ";
//		NDebugger::timer("managers");

		$db->query("
			UPDATE `out.salesman` s SET manager = (
				SELECT m.id FROM `out.managers` m WHERE m.name = s.manager
			)
		");

		$db->query("
			UPDATE `out.managerTargets` t SET manager = (
				SELECT m.id FROM `out.managers` m WHERE m.name = t.manager
			)
		");

		$this->log->info("Slevomat: Assigned managers to Salesman and ManagerTargets.");
		print "OK\n";
		return true;
	}
}
