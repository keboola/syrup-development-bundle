<?php
/**
 * IndexController.php
 *
 * @author: Miroslav Čillík <miro@keboola.com>
 * @created: 22.8.13
 */

namespace Syrup\CoreBundle\Controller;

use Composer\Config;
use Composer\Factory;
use Composer\IO\NullIO;
use Composer\Json\JsonFile;
use Composer\Package\Locker;
use Composer\Package\Package;
use Composer\Repository\ComposerRepository;
use Composer\Repository\FilesystemRepository;
use Composer\Repository\InstalledFilesystemRepository;
use Symfony\Bundle\FrameworkBundle\Controller\Controller;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\HttpFoundation\JsonResponse;

class IndexController extends Controller
{
	/**
	 * Displays Syrup components with their recent version
	 *
	 * @return JsonResponse
	 */
	public function indexAction()
	{
		$rootPath = str_replace('web/../', '', ROOT_PATH);

		$installedJson = new JsonFile($rootPath . '/vendor/composer/installed.json');
		$repo = new FilesystemRepository($installedJson);

		$syrupComponents = array();

		/** @var Package $package */
		foreach ($repo->getPackages() as $package) {

			$nameArr = explode("/", $package->getPrettyName());

			if ($nameArr[0] == 'syrup' || $nameArr[0] == 'keboola') {
				$syrupComponents[$package->getPrettyName()] = $package->getPrettyVersion();
			}
		}

		return new JsonResponse(array(
			"host"          => gethostname(),
			"components"    => $syrupComponents,
			"documentation" => "http://documentation.keboola.com/syrup"
		));
	}

}
