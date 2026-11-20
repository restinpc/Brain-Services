<?php
/** Additive registry cutover only; no DDL, history rewrite or consumer switch. */
if(PHP_SAPI!=='cli'||!preg_match('/^Uid:\s+0\s+0\s/m',file_get_contents('/proc/self/status')))throw new RuntimeException('ROOT_CLI_REQUIRED');
chdir('/brain/Brain-Server/public_html');
$_SERVER['DOCUMENT_ROOT']=getcwd();$_SERVER['HTTP_HOST']='localhost';
$_SERVER['REQUEST_URI']='/model100-context-capacity';$_SERVER['SCRIPT_NAME']='/index.php';
$_SERVER['SCRIPT_FILENAME']=getcwd().'/index.php';$_SERVER['EXE']=false;
define('DEBUG',1);define('DEEP_DEBUG',0);
ob_start();require 'engine/nodes/config.php';require 'engine/nodes/remote_compute_config.php';ob_end_clean();
require_once 'engine/core/engine.php';require_once 'engine/nodes/core_autoload.php';
require_once 'engine/core/BrainParserWriterFence.php';
$c=$_SERVER['config'];mysqli_report(MYSQLI_REPORT_ERROR|MYSQLI_REPORT_STRICT);
$db=new mysqli($c['sql_server'],$c['sql_login'],$c['sql_pass'],$c['sql_db']);
$db->set_charset('utf8mb4');$db->query("SET SESSION innodb_lock_wait_timeout=2");
if($db->query('SELECT @@hostname')->fetch_row()[0]!=='techno-kur-brain01.local')throw new RuntimeException('B1_ONLY');
$lease=BrainParserWriterFence::acquireForRegisteredTable('vlad_model100_h1_features',true);
if($lease===false)throw new RuntimeException('WRITER_BUSY');
try {
 if($db->query("SELECT GET_LOCK('model100-h1-feature-store',0)")->fetch_row()[0]!=1)throw new RuntimeException('WRITER_BUSY');
 $old='model100-h1-v2full-20261118';$new='model100-h1-v2full-20261120';
 $rows=$db->query("SELECT * FROM vlad_model100_h1_feature_sets WHERE dataset_version='$old' ORDER BY mode")->fetch_all(MYSQLI_ASSOC);
 if(count($rows)!==2)throw new RuntimeException('SOURCE_REGISTRY_REQUIRED');
 foreach($rows as&$r){$mode=$db->real_escape_string($r['mode']);$r['last_cutoff']=$db->query("SELECT MAX(cutoff_utc) FROM vlad_model100_h1_features WHERE dataset_version='$old' AND mode='$mode'")->fetch_row()[0];if(!$r['last_cutoff'])throw new RuntimeException('SOURCE_PROGRESS_REQUIRED');}unset($r);
 $python=<<<'PY'
import sys,json
from shared.model100_h1_features import digest,canonical
from shared.model100_rich_context import contract
from shared.model100_rich_runtime import code_hash,DATASET
assert DATASET=='model100-h1-v2full-20261120'
out=[]
for row in json.load(sys.stdin):
 schema=json.loads(row['schema_json'])
 assert digest(schema)==row['schema_hash'],'SOURCE_SCHEMA_DRIFT'
 previous=dict(schema['rich_contract'])
 assert previous['max_bytes']==524288,'SOURCE_TRANSPORT_LIMIT'
 previous['max_bytes']=1048576
 assert previous==contract(),'NON_TRANSPORT_CONTRACT_CHANGE'
 schema['rich_contract']=contract()
 out.append(dict(mode=row['mode'],schema_hash=digest(schema),schema_json=canonical(schema),builder_code_sha256=code_hash(),builder_version=row['builder_version'],status_json=canonical(dict(enabled=True,bootstrap_end_utc=row['last_cutoff']+'+00:00',bootstrap_rows=0,history_policy='LIVE_ONLY_CUTOVER',history_source_version=row['dataset_version'],lanes={},cutover_note='Full bounded context retained; transport ceiling 1 MiB. Prior rows and reader selection unchanged.'))))
print(json.dumps(out))
PY;
 $pipes=[];$proc=proc_open(['python3','-c',$python],[0=>['pipe','r'],1=>['pipe','w'],2=>['pipe','w']],$pipes,'/brain/Brain-Services');
 if(!is_resource($proc))throw new RuntimeException('PLAN_PROCESS_FAILED');
 fwrite($pipes[0],json_encode($rows,JSON_THROW_ON_ERROR));fclose($pipes[0]);
 $output=stream_get_contents($pipes[1]);fclose($pipes[1]);$error=stream_get_contents($pipes[2]);fclose($pipes[2]);$rc=proc_close($proc);
 if($rc!==0)throw new RuntimeException('PLAN_FAILED:'.substr($error,0,300));
 $plan=json_decode($output,true,512,JSON_THROW_ON_ERROR);
 if(($argv[1]??'')!=='--apply'){echo json_encode(['status'=>'DRY_RUN','dataset'=>$new,'modes'=>array_column($plan,'mode')])."\n";exit;}
 $path='/var/tmp/brain-model100-context-capacity-before-20261120.json';
 if(file_exists($path))throw new RuntimeException('RECEIPT_ALREADY_EXISTS');
 $f=fopen($path,'x');chmod($path,0600);fwrite($f,json_encode(['old'=>$rows,'plan'=>$plan,'at'=>gmdate('c')],JSON_THROW_ON_ERROR));fflush($f);fsync($f);fclose($f);
 $db->begin_transaction();
 try {
  foreach($plan as$p){
   $q=$db->prepare("INSERT INTO vlad_model100_h1_feature_sets(dataset_version,mode,schema_hash,schema_json,builder_code_sha256,builder_version,last_status,status_json) VALUES(?,?,?,?,?,?,'REGISTERED_TRANSPORT_CAPACITY_CUTOVER',?)");
   $q->bind_param('sssssss',$new,$p['mode'],$p['schema_hash'],$p['schema_json'],$p['builder_code_sha256'],$p['builder_version'],$p['status_json']);$q->execute();$q->close();
  }
  $db->commit();
 }catch(Throwable$e){$db->rollback();throw$e;}
 echo json_encode(['status'=>'APPLIED','dataset'=>$new,'registry_rows'=>count($plan),'model_rows_rewritten'=>0])."\n";
}finally{$db->query("DO RELEASE_LOCK('model100-h1-feature-store')");BrainParserWriterFence::release($lease);$db->close();}
