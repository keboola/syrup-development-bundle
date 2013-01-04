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
		if (isset($params['configuration'])) {
			$transformConfig = $params['configuration'];
		}

		$tables = array();
		$prefix = '';
		if (strstr($params['source'], ',')) {
			$tables = explode(',', $params['source']);
			$tables = array_map(function($item) {
				return trim($item);
			}, $tables);

			$prefixArr = explode('.', $tables[0]);
			$prefix = $prefixArr[0] . '.' . $prefixArr[1];
		} else {
			if ($this->_storageApi->bucketExists($params['source'])) {
				$tableList = $this->_storageApi->listTables($params['source']);
				foreach($tableList as $tab) {
					$tables[] = $tab['id'];
				}
				$prefix = $params['source'];
			}
		}

		if (!isset($transformConfig['import']) || $transformConfig['import'] == 1) {
			$this->_log->info("Importing source - START");
			$this->_import($tables);
			$this->_log->info("Importing source - END");
		}

		if (!isset($transformConfig['transform']) || $transformConfig['transform'] == 1) {
			$this->_log->info("Transforming - START");
			$this->_transform($prefix);
			$this->_log->info("Transforming - END");
		}

		if (!isset($transformConfig['export']) || $transformConfig['export'] == 1) {
			$this->_log->info("Exporting output - START");
			$this->_export($params['destination'], $prefix);
			$this->_log->info("Exporting output - END");
		}

		return false;
	}

	protected function _import($tables)
	{
		foreach($tables as $t) {

			$this->_log->info("Importing table " . $t);

			$table = $this->_storageApi->getTable($t);
			$tableName = $table['id'];

			$sql = "DROP TABLE IF EXISTS `" . $tableName . "`";
			$this->_db->query($sql);

			$sql = "CREATE TABLE `" . $tableName . "` (";
			foreach ($table['columns'] as $col) {
				$sql .= "`" . $col . "` TEXT,";
			}
			$sql = substr($sql, 0, -1);
			$sql .= ")";
			$this->_db->query($sql);

			$filename = "/tmp/" . $this->_prefix . '-' . $this->_name . '-' . $tableName . "_" . md5(microtime()) . ".csv";

			$this->_storageApi->exportTable($table['id'], $filename);

			$this->_db->query("
				LOAD DATA LOCAL INFILE '{$filename}' INTO TABLE `{$tableName}`
                FIELDS TERMINATED BY ',' ENCLOSED BY '\"'
                LINES TERMINATED BY '\n'
                IGNORE 1 LINES"
			);

			unlink($filename);
		}
	}

	protected function _transform($prefix)
	{
		$this->_initTransform($prefix);

		$this->_processCreateProductSnapshots($prefix);

		$this->_processAssignDealCategories($prefix);

		$this->_processAssignDealLimitsToProducts($prefix);

		// Days since last orders snapshots
		$this->_processDaysSinceLastOrderSnapshots($prefix);

		// Transform sales targets
		$this->_processTransformSalesTargets($prefix);

		// Assigning salesman to products
		$this->_processAssignSalesmenToProducts($prefix);

		// Update cancelled vouchers
		$this->_processUpdateCancelledVouchers($prefix);

		// Salesman Bonuses
		$this->_processComputeSalaries($prefix);

		// Manager and ManagerTargets
		$this->_processAssignManagers($prefix);

		// Assign city to orders
		$this->_processCityTargets($prefix);
	}

	protected function _export($destinationBucket, $prefix)
	{
		$dbConfig = $this->_db->getParams();

		if (!$this->_storageApi->bucketExists($destinationBucket)) {
			throw new \Exception("Destination bucket not found.");
		}
		// get tables from Slevomat_out
		$outTables = $this->_db->getSchemaManager()->listTableNames();

		foreach($outTables as $t) {

			if (!strstr($t, $prefix)) {
				continue;
			}

			$this->_log->info("Exporting table " . $t);

			$outTableName = str_replace($prefix . '.', '', $t);
			$outFilename = tempnam("/tmp", $outTableName) . ".csv";
			$errorFilename = tempnam("/tmp", $outTableName) . ".csv.error";

			$select = "SELECT * FROM `{$t}`;";

			$command = 'mysql -u ' . $dbConfig['user']
				. ' -p' . $dbConfig['password']
				. ' -h ' . $dbConfig['host']
				. ' '. $dbConfig['dbname'] . ' -B -e ' . escapeshellarg((string) $select)
				. ' --quick 2> ' . $errorFilename;

			$conversionPath = ROOT_PATH . "src/Syrup/SlevomatBundle/Scripts/conversion.php";
			$command .= ' | php ' . $conversionPath . " csv";
			$command .= ' > ' . $outFilename;

			$result = exec($command);

			if ($result != "" || file_exists($errorFilename) && filesize($errorFilename) > 0) {
				$error = $result;
				if ($error == '') {
					$error = trim(file_get_contents($errorFilename));
				}
				throw new \Exception("MySQL export error: " . $error);
			}

			$this->_log->info("Writing table " . $t . " to Storage API");

			if (!$this->_storageApi->tableExists($destinationBucket . '.' . $outTableName)) {
				$this->_storageApi->createTable($destinationBucket, $outTableName, $outFilename);
			} else {
				$this->_storageApi->writeTable($destinationBucket . '.' . $outTableName, $outFilename);
			}

			$this->_log->info("Table " . $t . " exported to Storage API");
		}
	}

	protected function _initTransform($prefix)
	{
		$this->_db->query("
			ALTER TABLE `{$prefix}.users` ADD INDEX `id` (`id`(11));
		");
		$this->_db->query("
			ALTER TABLE `{$prefix}.carts`
			ADD INDEX `id` (`id`(11)),
			ADD INDEX `user` (`user`(11));
		");

		$this->_db->query("
			ALTER TABLE `{$prefix}.cityTargets`
			ADD PRIMARY KEY `id` (`id`(11)),
			ADD INDEX `city` (`city`(11));
		");

		$this->_db->query("
			ALTER TABLE `{$prefix}.daysSinceLastOrderSnapshot`
			CHANGE `id` `id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;
		");

		$this->_db->query("
			ALTER TABLE `{$prefix}.dealLimits`
			CHANGE `id` `id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST,
			CHANGE `name` `name` text COLLATE 'utf8_general_ci' NOT NULL AFTER `id`,
			CHANGE `namesort` `namesort` int unsigned NOT NULL AFTER `name`,
			CHANGE `limit` `limit` int unsigned NOT NULL AFTER `namesort`,
			CHANGE `limitMinimum` `limitMinimum` int unsigned NOT NULL AFTER `limit`,
			CHANGE `limitWarning` `limitWarning` int unsigned NOT NULL AFTER `limitMinimum`;
		");

		$this->_db->query("
			ALTER TABLE `{$prefix}.marginLimits`
			ADD INDEX `id` (`id`(11)),
			ADD INDEX `salesman` (`salesman`(11));
		");
		$this->_db->query("
			ALTER TABLE `{$prefix}.newsletters`
			ADD INDEX 	`deleted` (`deleted`(11));
		");
		$this->_db->query("
			ALTER TABLE `{$prefix}.orders`
			CHANGE `id` `id` int unsigned NOT NULL AUTO_INCREMENT FIRST,
			CHANGE `dateTime` `dateTime` datetime NOT NULL AFTER `id`,
			CHANGE `paymentTime` `paymentTime` datetime NULL AFTER `dateTime`,
			CHANGE `totalPrice` `totalPrice` decimal(16,2) NOT NULL AFTER `amount`,
			CHANGE `commission` `commission` decimal(16,2) NOT NULL AFTER `paidPrice`,
			CHANGE `cart` `cart` int unsigned NULL AFTER `shippingType`,
			CHANGE `product` `product` int unsigned NOT NULL AFTER `cart`,
			CHANGE `user` `user` int unsigned NULL AFTER `product`,
			CHANGE `variant` `variant` int unsigned NULL AFTER `user`,
			CHANGE `cancelled` `cancelled` varchar(255) COLLATE 'utf8_general_ci' NULL AFTER `paid`,
			CHANGE `cancelledVoucher` `cancelledVoucher` varchar(255) COLLATE 'utf8_general_ci' NULL AFTER `city`,
			ADD INDEX `user` (`user`),
			ADD INDEX `product` (`product`),
			ADD INDEX `variant` (`variant`),
			ADD INDEX `dateTime` (`dateTime`),
			ADD INDEX `user_id` (`user`,`id`),
			ADD INDEX `id_user` (`id`,`user`),
			ADD INDEX `paid` (`paid`(11)),
			ADD INDEX `cart` (`cart`),
			ADD INDEX `cancelled` (`cancelled`),
			ADD INDEX `cancelledVoucher` (`cancelledVoucher`);
		");
		$this->_db->query("
			ALTER TABLE `{$prefix}.products`
			CHANGE `id` `id` int unsigned NOT NULL PRIMARY KEY FIRST,
			CHANGE `start` `start` datetime NOT NULL AFTER `category`,
			CHANGE `end` `end` datetime NOT NULL AFTER `start`,
			ADD INDEX `partner` (`partner`(11)),
			ADD INDEX `salesmanId` (`salesmanId`(11)),
			ADD INDEX `dealCategory` (`dealCategory`(11));
		");

		$this->_db->query("
			ALTER TABLE `{$prefix}.variants`
			ADD INDEX `id` (`id`(11)),
			ADD INDEX `product` (`product`(11));
		");

		$this->_db->query("
			ALTER TABLE `{$prefix}.vouchers`
			CHANGE `id` `id` int unsigned NOT NULL FIRST,
			CHANGE `order` `order` int unsigned NOT NULL AFTER `id`,
			ADD INDEX `id` (`id`),
			ADD INDEX `order` (`order`);
		");

		$this->_db->query("
			ALTER TABLE `{$prefix}.partners`
			ADD INDEX `id` (`id`(11));
		");

		$this->_db->query("
			ALTER TABLE `{$prefix}.managers`
			CHANGE `id` `id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;
		");

		$this->_db->query("
			ALTER TABLE `{$prefix}.marginLimits`
			CHANGE `id` `id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;
		");

		$this->_db->query("
			ALTER TABLE `{$prefix}.salesTargets`
			CHANGE `id` `id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST,
			CHANGE `date` `date` date NOT NULL AFTER `value`;
		");

		$this->_db->query("
			ALTER TABLE `{$prefix}.salesman`
			CHANGE `id` `id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;
		");

		$this->_db->query("
			ALTER TABLE `{$prefix}.salesmanBonus`
			CHANGE `id` `id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST,
			CHANGE `salesman` `salesman` int unsigned NOT NULL AFTER `id`,
			CHANGE `date` `date` date NOT NULL AFTER `salesman`,
			CHANGE `salarymonth` `salarymonth` date NOT NULL AFTER `date`,
			CHANGE `h1` `h1` decimal(16,2) NOT NULL DEFAULT '0' AFTER `salarymonth`,
			CHANGE `h1count` `h1count` int unsigned NOT NULL DEFAULT '0' AFTER `h1`,
			CHANGE `h2` `h2` decimal(16,2) NOT NULL DEFAULT '0' AFTER `h1count`,
			CHANGE `h3` `h3` decimal(16,2) NOT NULL DEFAULT '0' AFTER `h2`,
			CHANGE `h4` `h4` decimal(16,2) NOT NULL DEFAULT '0' AFTER `h3`,
			CHANGE `h5` `h5` decimal(16,2) NOT NULL DEFAULT '0' AFTER `h4`,
			CHANGE `h6` `h6` decimal(16,2) NOT NULL DEFAULT '0' AFTER `h5`,
			CHANGE `total` `total` decimal(16,2) NOT NULL DEFAULT '0' AFTER `h6`,
			CHANGE `citiesMargin` `citiesMargin` decimal(16,2) NOT NULL DEFAULT '0' AFTER `total`,
			CHANGE `tabsMargin` `tabsMargin` decimal(16,2) NOT NULL DEFAULT '0' AFTER `citiesMargin`,
			CHANGE `target` `target` decimal(4,3) NOT NULL DEFAULT '0' AFTER `tabsMargin`,
			ADD INDEX `salesman` (`salesman`);
		");

		$this->_db->query("
			ALTER TABLE `{$prefix}.targetFulfillments`
			CHANGE `id` `id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST,
			CHANGE `day` `day` date NOT NULL AFTER `id`;
		");
	}

	/**
	 *
	 * create product snapshots
	 * @return bool
	 */
	protected function _processCreateProductSnapshots($prefix)
	{
		$db = $this->_db;
		// calculate additional data

		$db->query("DROP TABLE IF EXISTS `{$prefix}.productsSnapshots`;");
		$db->query("CREATE TABLE `{$prefix}.productsSnapshots` (
                `id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
                `date` date NOT NULL,
                `product` int unsigned NOT NULL,
                `price` decimal(16,2) NOT NULL DEFAULT '0',
                `commission` decimal(16,2) NOT NULL DEFAULT '0'
			) COMMENT='' ENGINE='InnoDB' COLLATE 'utf8_general_ci';");

		$products = $db->fetchAll("SELECT id, start, end, price, commission FROM `{$prefix}.products`");

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
			$query = "INSERT INTO `{$prefix}.productsSnapshots` (`date`, product, price, commission) VALUES ";
			$queryArr = array();
			foreach ($dates as $date) {
				$queryArr[] = "('{$date}', {$product["id"]}, '{$product["price"]}', '{$product["commission"]}')";
			}
			$db->query($query . join(", ", $queryArr));

		}
		$this->_log->info("Slevomat: Created products-date snapshots.");
		return true;
	}

	/**
	 *
	 * Assign deal categories to products
	 *
	 * @param $db
	 * @return bool
	 */
	protected function _processAssignDealCategories($prefix) {
		$db = $this->_db;

		$products = $db->fetchAll("SELECT id, cities FROM `{$prefix}.products`");
		foreach ($products as $product) {
			if ($product["cities"] == "Praha") {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Praha', dealCategorySort = 1 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Praha Extra") {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Praha Extra', dealCategorySort = 2 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Brno") {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Brno', dealCategorySort = 3 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Ostrava") {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Ostrava', dealCategorySort = 4 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Olomouc a Haná") {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Olomouc a Haná', dealCategorySort = 5 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "HK a Pardubice") {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'HK a Pardubice', dealCategorySort = 6 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Zlín a okolí") {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Zlín a okolí', dealCategorySort = 7 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Plzeň") {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Plzeň', dealCategorySort = 8 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Liberec") {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Liberec', dealCategorySort = 9 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Karlovy Vary") {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Karlovy Vary', dealCategorySort = 10 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "České Budějovice") {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'České Budějovice', dealCategorySort = 11 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Ústí n. L. a Teplice") {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Ústí n. L. a Teplice', dealCategorySort = 12 WHERE id = {$product["id"]}");
				continue;
			}
			if ($product["cities"] == "Jihlava a Vysočina") {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Jihlava a Vysočina', dealCategorySort = 13 WHERE id = {$product["id"]}");
				continue;
			}
			if (strpos($product["cities"], "Móda") !== false) {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Móda', dealCategorySort = 14 WHERE id = {$product["id"]}");
				continue;
			}
			if (strpos($product["cities"], "Cestování") !== false) {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Cestování', dealCategorySort = 15 WHERE id = {$product["id"]}");
				continue;
			}
			if (strpos($product["cities"], "Zboží") !== false) {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Zboží', dealCategorySort = 16 WHERE id = {$product["id"]}");
				continue;
			}
			if (strpos($product["cities"], "Víno") !== false) {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Víno', dealCategorySort = 17 WHERE id = {$product["id"]}");
				continue;
			}
			if (strpos($product["cities"], "Praha") !== false || strpos($product["cities"], "Praha Extra") !== false) {
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Pha', dealCategorySort = 18 WHERE id = {$product["id"]}");
				continue;
			}
			if (strpos($product["cities"], "Brno") !== false
				|| strpos($product["cities"], "Ostrava") !== false
				|| strpos($product["cities"], "Plzeň") !== false
			)
			{
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'R1', dealCategorySort = 19 WHERE id = {$product["id"]}");
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
				$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'R2', dealCategorySort = 20 WHERE id = {$product["id"]}");
				continue;
			}
			$db->query("UPDATE `{$prefix}.products` SET dealCategory = 'Other', dealCategorySort = 21 WHERE id = {$product["id"]}");
		}
		$this->_log->info("Slevomat: Assigned deal categories.");

		return true;
	}

	/**
	 *
	 * Assign deal limits to products
	 *
	 * @param $db
	 * @return bool
	 */
	protected function _processAssignDealLimitsToProducts($prefix) {
		$db = $this->_db;

		$db->query("DROP TABLE IF EXISTS `{$prefix}.productsDealLimits`;");
		$db->query("
			CREATE TABLE `{$prefix}.productsDealLimits` (
			  `product` int(10) unsigned NOT NULL,
			  `dealLimit` int(10) unsigned NOT NULL,
			  PRIMARY KEY (`product`,`dealLimit`),
			  KEY `dealLimit` (`dealLimit`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8;
		");
		$limits = $db->fetchAll("SELECT id, `name` FROM `{$prefix}.dealLimits` ORDER BY namesort");
		foreach($limits as $limit) {
			$db->query("
				INSERT INTO `{$prefix}.productsDealLimits`
				SELECT products.id, '{$limit["id"]}' FROM `{$prefix}.products` products
				LEFT JOIN `{$prefix}.productsDealLimits` ON products.id = `{$prefix}.productsDealLimits`.product
				WHERE `{$prefix}.productsDealLimits`.product IS NULL AND dealCategory = '{$limit["name"]}'
			");
		}
		$this->_log->info("Slevomat: Assigned deal limits.");

		return true;
	}

	/**
	 *
	 * Compute days since last order snapshots for each user
	 *
	 * @param $db
	 * @return bool
	 */
	protected function _processDaysSinceLastOrderSnapshots($prefix) {
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
		$insertedDates = $db->fetchAll("SELECT DISTINCT `date` FROM `{$prefix}.daysSinceLastOrderSnapshot`");
		$datesToInsert = array_diff ($dates, (array_map(function($array) {return $array["date"];}, $insertedDates)));

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
				FROM `{$prefix}.users` t
				LEFT JOIN `{$prefix}.orders` o ON o.user = t.id AND o.dateTime < '{$date}'
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
				INSERT INTO `{$prefix}.daysSinceLastOrderSnapshot` (`date`, `category`, `categorySort`, `numberofusers`)
				VALUES
				('{$date}', '--ještě neobjednal--', 4, '{$data['--ještě neobjednal--']}'),
				('{$date}', '> 12 měsíců', 3, '{$data['> 12 měsíců']}'),
				('{$date}', '6-12 měsíců', 2, '{$data['6-12 měsíců']}'),
				('{$date}', '3-6 měsíců', 1, '{$data['3-6 měsíců']}'),
				('{$date}', '0-3 měsíce', 0, '{$data['0-3 měsíce']})');");

			$this->_log->info("Slevomat: Computed daysSinceLastOrder snapshot for {$date}.");

		}
		$this->_log->info("Slevomat: Computed daysSinceLastOrder snapshots.");

		return true;
	}

	/**
	 *
	 * Assign salesmen to products
	 *
	 * @param $db
	 * @return bool
	 */
	protected function _processAssignSalesmenToProducts($prefix)
	{
		$db = $this->_db;

		$salesmen = $db->fetchAll("SELECT id, name FROM `{$prefix}.salesman` WHERE name != '--empty--'");
		$emptySalesman = $db->fetchArray("SELECT id, name FROM `{$prefix}.salesman` WHERE name = '--empty--'");

		$salesmanNames = array();
		foreach ($salesmen as $salesman) {
			$db->query("UPDATE `{$prefix}.products` SET salesmanId = " . $salesman['id'] . " WHERE salesman = '{$salesman["name"]}'");
			$salesmanNames[] = "'{$salesman["name"]}'";
		}

		$db->query("UPDATE `{$prefix}.products` SET salesmanId = {$emptySalesman[0]} WHERE salesman NOT IN (" . join(",", $salesmanNames) . ")");

		$this->_log->info("Slevomat: Assigned salesman to products.");

		return true;

	}


	/**
	 *
	 * Compute salesmen salaries
	 *
	 * @param $db
	 * @return bool
	 */
	protected function _processComputeSalaries($prefix)
	{
		// Computing salesman salaries
		$db = $this->_db;

		// Truncate table salesmanBonus - if truncated, it will be computed from scratch
		//$db->query("TRUNCATE TABLE `{$prefix}.salesmanBonus`");

		// All missing dates
		$lastDate = $db->fetchColumn("SELECT IFNULL(MAX(`date`), '2012-04-01') FROM `{$prefix}.salesmanBonus`;");
		$dateTo = date("Y-m-d");
		$currentDate = $lastDate;
		if ($lastDate != $dateTo) {
			while($currentDate <= $dateTo) {
				// H1, H2, H3, H4, H6
				$this->_computeSalaries($currentDate, $prefix);

				$currentDate = date("Y-m-d", strtotime("+1 day", strtotime($currentDate)));
			}
		}

		// H5 - compute from start
		$currentDate = date("Y-m-d", strtotime("2012-04-01"));
		while($currentDate <= $dateTo) {
			$this->_computeH5($currentDate, $prefix);
			$currentDate = date("Y-m-d", strtotime("+1 day", strtotime($currentDate)));
		}

		// Targets
		$this->_computeTargetFulfillments($prefix);

		$this->_log->info("Slevomat: Computed salesmen salaries.");

		return true;
	}

	/**
	 *
	 * Internal routine to calculate salaries for a specific day
	 *
	 * @param Zend_Db_Adapter_Abstract $db
	 * @param $currentDate string
	 */
	private function _computeSalaries($currentDate, $prefix)
	{
		$db = $this->_db;

		$salaryMonths =  array(date("Y-m", strtotime($currentDate)));
		if (date("d", strtotime($currentDate)) <= 13) {
			$salaryMonths[] = date("Y-m", strtotime("-1 month", strtotime($currentDate)));
		}

		$salesmen = $db->fetchAll("SELECT id, name, region FROM `{$prefix}.salesman` WHERE name != '--empty--'");

		$phoniesRes = $db->fetchAll("SELECT salesman FROM `{$prefix}.phones`");
		$phonies = array_map(function($item) {
			return $item['salesman'];
		}, $phoniesRes);

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
							FROM `{$prefix}.orders` orders
							LEFT JOIN `{$prefix}.products` products ON orders.product = products.id
							LEFT JOIN `{$prefix}.productsDealLimits` productsDealLimits ON products.id = productsDealLimits.product
							LEFT JOIN `{$prefix}.dealLimits` dealLimits ON productsDealLimits.dealLimit = dealLimits.id
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
							FROM `{$prefix}.orders` orders
							LEFT JOIN `{$prefix}.products` products ON orders.product = products.id
							LEFT JOIN `{$prefix}.productsDealLimits` productsDealLimits ON productsDealLimits.product = products.id
							LEFT JOIN `{$prefix}.dealLimits` dealLimits ON productsDealLimits.dealLimit = dealLimits.id
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
							FROM `{$prefix}.orders` orders
							LEFT JOIN `{$prefix}.products` products ON orders.product = products.id
							LEFT JOIN `{$prefix}.productsDealLimits` productsDealLimits ON productsDealLimits.product = products.id
							LEFT JOIN `{$prefix}.dealLimits` dealLimits ON productsDealLimits.dealLimit = dealLimits.id
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
					FROM `{$prefix}.orders` orders LEFT JOIN `{$prefix}.products` products ON orders.product = products.id
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
					FROM `{$prefix}.orders` orders LEFT JOIN `{$prefix}.products` products ON orders.product = products.id
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
				$personalLimit = $db->fetchArray("SELECT marginLimit, bonus FROM `{$prefix}.marginLimits` WHERE month = '{$salaryMonth}-01}' AND salesman = {$salesman["id"]}");

				if ($personalLimit["marginLimit"] <= ($tabbedProducts + $normalProducts)) {
					$bonuses["H3"] += $personalLimit["bonus"];
				} elseif ($personalLimit["marginLimit"] * 0.75 <= ($tabbedProducts + $normalProducts)) {
					$bonuses["H3"] += $personalLimit["bonus"] * 0.5;
				}

				// H4 bonus
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
							FROM `{$prefix}.orders` orders
							LEFT JOIN `{$prefix}.products` products ON orders.product = products.id
							LEFT JOIN `{$prefix}.productsDealLimits` productsDealLimits ON products.id = productsDealLimits.product
							LEFT JOIN `{$prefix}.dealLimits` dealLimits ON productsDealLimits.dealLimit = dealLimits.id
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
				$row = $db->fetchAssoc("SELECT * FROM `{$prefix}.salesmanBonus` WHERE salesman = {$salesman["id"]} AND `date` = '{$currentDate}' AND salarymonth = '{$salaryMonth}-01'");
				if (!$row) {
					$db->query("
						INSERT INTO `{$prefix}.salesmanBonus` SET
							salesman = '{$salesman["id"]}',
							`date` = '{$currentDate}',
							salarymonth = '{$salaryMonth}-01',
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
						UPDATE `{$prefix}.salesmanBonus` SET
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
	protected function _computeH5($currentDate, $prefix)
	{
		$db = $this->_db;

		$salesmanBonuses = $db->fetchAll("
			SELECT sb.id, sb.salesman, sb.date, sb.salarymonth, s.team, sb.citiesMargin + sb.tabsMargin AS salesmanCommission FROM `{$prefix}.salesmanBonus` sb
			LEFT JOIN `{$prefix}.salesman` s ON (sb.salesman = s.id)
			WHERE (sb.citiesMargin != 0 OR sb.tabsMargin != 0) AND `date` = '{$currentDate}' AND team != 1000
		");

		foreach ($salesmanBonuses as $salesmanBonusRow) {

			$teamTarget = $db->fetchColumn("SELECT value FROM `{$prefix}.teamTargets` WHERE `key` = 'Marze bez DPH' AND month = '" . $salesmanBonusRow['salarymonth'] . "' AND team='" . $salesmanBonusRow['team'] . "'");
			$teamBonus = $db->fetchColumn("SELECT value FROM `{$prefix}.teamTargets` WHERE `key` = 'Bonus' AND month = '" . $salesmanBonusRow['salarymonth'] . "' AND team='" . $salesmanBonusRow['team'] . "'");

			$teamCommission = $db->fetchColumn("
				SELECT SUM(m.teamMargin) AS teamCommission FROM (
					SELECT MAX(sb.citiesMargin) + MAX(sb.tabsMargin) AS teamMargin FROM `{$prefix}.salesmanBonus` sb
					LEFT JOIN `{$prefix}.salesman` s ON (sb.salesman = s.id)
					WHERE s.team = '" .$salesmanBonusRow['team'] . "'
					AND sb.salarymonth = '" . $salesmanBonusRow['salarymonth'] . "'
					GROUP BY sb.salesman
				) m
			");

			$salesmanCommission = $salesmanBonusRow['salesmanCommission'];

			if ($teamCommission >= $teamTarget) {
				$bonus = $teamBonus * ($salesmanCommission / $teamCommission);
				$db->query("
					UPDATE `{$prefix}.salesmanBonus`
					SET h5 = " . $bonus . ",
					total = total + " . $bonus . "
					WHERE id='" . $salesmanBonusRow['id'] . "'
				");
			} else if ($teamCommission >= ($teamTarget * 0.75) ) {
				$bonus = $teamBonus * $salesmanCommission / $teamCommission * 0.5;
				$db->query("
					UPDATE `{$prefix}.salesmanBonus`
					SET h5 = " . $bonus . ",
					total = total + " . $bonus . "
					WHERE id='" . $salesmanBonusRow['id'] . "'
				");
			}
		}
	}

	/**
	 *
	 * @return boolean
	 */
	protected function _computeTargetFulfillments($prefix)
	{
		$db = $this->_db;

		$db->query("
			UPDATE `{$prefix}.salesmanBonus` SET target = (
				SELECT tf.value FROM `{$prefix}.targetFulfillments` tf WHERE tf.id=DAY(`{$prefix}.salesmanBonus`.date)
			) WHERE MONTH(date)=MONTH(salarymonth)
		");
		$db->query("
			UPDATE `{$prefix}.salesmanBonus` SET target = (
				SELECT tf.value FROM `{$prefix}.targetFulfillments` tf WHERE tf.id=(DAY(`{$prefix}.salesmanBonus`.date) + 31)
			) WHERE MONTH(date)!=MONTH(salarymonth)
		");
		// if (date - salarymonth > 31 + 12) target = 100%
		$db->query("
			UPDATE `{$prefix}.salesmanBonus` SET target = 1.00
			WHERE MONTH(date)!=MONTH(salarymonth) AND DAY(date) > 12
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
	protected function _processTransformSalesTargets($prefix)
	{
		// Sales targets
		$db = $this->_db;

		// Create all salesman
		$db->query("TRUNCATE `{$prefix}.salesman`;");
		$db->query("
			INSERT INTO `{$prefix}.salesman`
			SELECT DISTINCT NULL, st.salesPerson, st.manager, st.region, IF (tm.id IS NULL, 1000, tm.id) AS team FROM `{$prefix}.salesTargets` st
			LEFT JOIN `{$prefix}.salesman` s ON s.name = st.salesPerson
			LEFT JOIN `{$prefix}.teams` tm ON st.team = tm.name
			WHERE s.id IS NULL;
		");
		$db->query("INSERT INTO `{$prefix}.salesman` SET id = 1000, name = '--empty--', manager = '--empty--', region = '---', team = 1000;");

		// Get the limits and insert into marginLimits
		$db->query("TRUNCATE `{$prefix}.marginLimits`;");
		$salesmen = $db->fetchAll("SELECT id, name FROM `{$prefix}.salesman` WHERE id != 1000");
		foreach ($salesmen as $salesman) {
			$dates = $db->fetchAll("SELECT DISTINCT `date` FROM `{$prefix}.salesTargets` WHERE salesPerson = '{$salesman["name"]}';");
			foreach ($dates as $date) {
				$limit = $db->fetchColumn("SELECT value FROM `{$prefix}.salesTargets` WHERE salesPerson = '{$salesman["name"]}' AND `date` = '{$date["date"]}' AND `key` = 'Marze bez DPH'");
				$bonus = $db->fetchColumn("SELECT value FROM `{$prefix}.salesTargets` WHERE salesPerson = '{$salesman["name"]}' AND `date` = '{$date["date"]}' AND `key` = 'Bonus'");
				$db->query("INSERT INTO `{$prefix}.marginLimits` SET salesman = '{$salesman["id"]}', `month` = '{$date["date"]}', marginLimit = '{$limit}', bonus = '{$bonus}'");
			}
		}

		$this->_log->info("Slevomat: Transformed sales targets.");
		return true;
	}


	/**
	 *
	 * Flags orders that have a cancelled vouchers
	 *
	 * @param $db
	 * @return bool
	 */
	protected function _processUpdateCancelledVouchers($prefix)
	{
		$db = $this->_db;

		// Update all orders with cancelled vouchers
		$db->query("UPDATE `{$prefix}.orders` SET cancelledVoucher = 'Yes' WHERE id IN(SELECT DISTINCT `{$prefix}.vouchers`.order FROM `{$prefix}.vouchers` WHERE cancelled = 'Yes')");

		$this->_log->info("Slevomat: Updated cancelled vouhcers.");
		return true;
	}

	/**
	 * @return bool
	 * @deprecated
	 */
	protected function _processNewslettersToOrders($prefix)
	{
		$db = $this->_db;

		// Update all orders where user is subscribed to some newsletter
		$db->query("UPDATE `{$prefix}.orders` SET newsletter = 1 WHERE `user` IN (SELECT DISTINCT `{$prefix}.newsletters`.user FROM `{$prefix}.newsletters` WHERE deleted = 'No')");

		$this->_log->info("Slevomat: Updated orders with newsletter flag.");
		return true;
	}

	protected function _processCityTargets($prefix)
	{
		$db = $this->_db;

		$db->query("
			UPDATE `{$prefix}.orders` o SET city = (
				SELECT ct.id FROM `{$prefix}.cityTargets` ct WHERE ct.city = o.city
			)
		");

		$this->_log->info("Slevomat: Assigned cities to orders.");
		return true;
	}

	protected function _processAssignManagers($prefix)
	{
		$db = $this->_db;

		$db->query("TRUNCATE `{$prefix}.managers`");
		$db->query("
			INSERT INTO `{$prefix}.managers` (name)
			SELECT DISTINCT manager FROM `{$prefix}.salesman` WHERE manager != '--empty--'
		");
		$db->query("
			REPLACE INTO `{$prefix}.managers`
			SET id = 1000, name = '--empty--'
		");

		$this->_log->info("Slevomat: Created teams tables.");

		$db->query("
			UPDATE `{$prefix}.salesman` s SET manager = (
				SELECT m.id FROM `{$prefix}.managers` m WHERE m.name = s.manager
			)
		");

		$db->query("
			UPDATE `{$prefix}.managerTargets` t SET manager = (
				SELECT m.id FROM `{$prefix}.managers` m WHERE m.name = t.manager
			)
		");

		$this->_log->info("Slevomat: Assigned managers to Salesman and ManagerTargets.");
		return true;
	}
}
