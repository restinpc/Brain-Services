<?php
/** Additive B1-only root migration. Rollback: disable V2; keep columns/data. */
if(PHP_SAPI!=='cli'||!preg_match('/^Uid:\s+0\s+0\s/m',file_get_contents('/proc/self/status')))throw new RuntimeException('ROOT_CLI_REQUIRED');
chdir('/brain/Brain-Server/public_html');
$_SERVER['DOCUMENT_ROOT']=getcwd();$_SERVER['HTTP_HOST']='localhost';$_SERVER['REQUEST_URI']='/model100-rich';
$_SERVER['SCRIPT_NAME']='/index.php';$_SERVER['SCRIPT_FILENAME']=getcwd().'/index.php';$_SERVER['EXE']=false;
define('DEBUG',1);define('DEEP_DEBUG',0);
require_once 'engine/nodes/config.php';require_once 'engine/nodes/remote_compute_config.php';
require_once 'engine/core/engine.php';require_once 'engine/nodes/core_autoload.php';
require_once 'engine/core/BrainParserWriterFence.php';require_once 'engine/core/BrainParserHealthMonitor.php';
if(($_SERVER['config']['node_name']??'')!=='Brain 1')throw new RuntimeException('B1_ONLY');
$action=$argv[1]??'dry-run';$table='vlad_model100_h1_features';
$base='/brain/model100-featurebuilder-20261109/v2-runtime';
if($action==='verify-v1'){
 engine::mysql("SET time_zone='+00:00'");$g=gzopen($base.'/v1-before.jsonl.gz','rb');if(!$g)throw new RuntimeException('BACKUP_REQUIRED');
 $hash=hash_init('sha256');$n=0;$pending=[];
 $verify=function($batch)use(&$hash,&$n,$table){
  foreach($batch as $old){
   $mode=engine::escape_string($old['mode']);$cut=engine::escape_string($old['cutoff_utc']);
   $columns=implode(',',array_keys($old));
   $r=engine::mysql("SELECT $columns FROM $table WHERE dataset_version='model100-h1-v1hex-20261109' AND mode='$mode' AND cutoff_utc='$cut'");
   $now=mysqli_fetch_assoc($r);if($now!==$old)throw new RuntimeException('V1_ROW_CHANGED:'.$mode.':'.$cut);
   hash_update($hash,json_encode($now,JSON_UNESCAPED_SLASHES|JSON_THROW_ON_ERROR)."\n");$n++;
  }
 };
 while(($line=gzgets($g))!==false){$pending[]=json_decode($line,true,512,JSON_THROW_ON_ERROR);if(count($pending)>=128){$verify($pending);$pending=[];}}
 if($pending)$verify($pending);gzclose($g);$sha=hash_final($hash);
 $m=json_decode(file_get_contents($base.'/v1-before-manifest.json'),true,512,JSON_THROW_ON_ERROR);
 if($sha!==$m['logical_sha256']||$n!==$m['rows'])throw new RuntimeException('V1_BASELINE_SHA_MISMATCH');
 echo json_encode(['v1_rows_unchanged'=>$n,'logical_sha256'=>$sha,'formed_at_included'=>true]).PHP_EOL;exit;
}
if($action==='history'){
 $r=engine::mysql("SELECT id,script,`table`,argument,date FROM brain_parser WHERE id=168");$row=mysqli_fetch_assoc($r);
 if(!$row||$row['table']!==$table||$row['script']!=='model100_feature_store_parser.py')throw new RuntimeException('REGISTRY_MISMATCH');
 $src=file_get_contents('engine/code/parser.php');$a=strpos($src,'function parser_run_registered_row(');$b=strpos($src,'function parser()');
 if($a===false||$b===false)throw new RuntimeException('RUNNER_MISSING');
 eval(substr($src,$a,$b-$a));$row['argument']='v2-history';
 echo json_encode(['native_runner'=>parser_run_registered_row($row),'parser_id'=>168]).PHP_EOL;exit;
}
if(!in_array($action,['dry-run','apply'],true))throw new RuntimeException('INVALID_ACTION');
$r=engine::mysql("SELECT COLUMN_NAME AS col_name,COLUMN_TYPE AS col_type,IS_NULLABLE AS nullable FROM information_schema.columns WHERE table_schema='brain' AND table_name='$table'");
$cols=[];while($v=mysqli_fetch_assoc($r))$cols[$v['col_name']]=$v;
$r=engine::mysql("SHOW CREATE TABLE $table");$ddl=mysqli_fetch_row($r)[1];
$ddlHash=hash('sha256',$ddl);
echo json_encode(['action'=>$action,'ddl_before_sha256'=>$ddlHash,'rich_column_exists'=>isset($cols['rich_context']),
                 'operation'=>'ADD_NULLABLE_COLUMNS_INSTANT_ONLY','rollback'=>'KEEP_V1_AND_ADDITIVE_COLUMNS_DISABLE_V2','replication'=>'UNCHANGED_B1_ONLY']).PHP_EOL;
if($action==='dry-run')exit;
$lease=BrainParserWriterFence::acquireForRegisteredTable($table,true);if($lease===false)throw new RuntimeException('WRITER_BUSY');
try{
 chgrp($base,48);chmod($base,02770);
 foreach(glob($base.'/*.json*') as $file){if(str_starts_with(basename($file),'v1-before'))continue;chgrp($file,48);chmod($file,0640);}
 engine::mysql("SET time_zone='+00:00'");engine::mysql('SET SESSION lock_wait_timeout=5');
 // Stream old logical rows in bounded pages, including formed_at and every old
 // field. Never overwrite a previous baseline. This is an audit/restore backup.
 $path=$base.'/v1-before.jsonl.gz';
 if(!file_exists($path)){
  $tmp=$path.'.partial';if(file_exists($tmp))throw new RuntimeException('INCOMPLETE_BACKUP_REVIEW_REQUIRED');
  $gz=gzopen($tmp,'wb6');$hash=hash_init('sha256');$n=0;
  foreach(['RESEARCH','STRICT_ASOF'] as $mode){
   $last='1000-01-01';
   do{
    $r=engine::mysql("SELECT dataset_version,mode,cutoff_utc,schema_hash,feature_values,missing_masks,provenance,row_hash,payload_sha256,builder_version,value_encoding,formed_at_utc FROM $table WHERE dataset_version='model100-h1-v1hex-20261109' AND mode='$mode' AND cutoff_utc>'$last' ORDER BY cutoff_utc LIMIT 128");
    $count=0;while($v=mysqli_fetch_assoc($r)){$line=json_encode($v,JSON_UNESCAPED_SLASHES|JSON_THROW_ON_ERROR)."\n";if(gzwrite($gz,$line)!==strlen($line))throw new RuntimeException('BACKUP_WRITE_FAILED');hash_update($hash,$line);$last=$v['cutoff_utc'];$count++;$n++;}
   }while($count===128);
  }
  if(!gzclose($gz)||!rename($tmp,$path))throw new RuntimeException('BACKUP_CLOSE_FAILED');chmod($path,0600);
  file_put_contents($base.'/v1-before-manifest.json',json_encode(['rows'=>$n,'logical_sha256'=>hash_final($hash),'file_sha256'=>hash_file('sha256',$path),'ddl_sha256'=>$ddlHash],JSON_PRETTY_PRINT));
  file_put_contents($base.'/table-before.sql',$ddl.";\n");
 }
 if(!isset($cols['rich_context'])){
  engine::mysql("ALTER TABLE $table ADD COLUMN rich_context JSON NULL, ADD COLUMN rich_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL, ADD COLUMN context_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL, ALGORITHM=INSTANT");
 }else foreach(['rich_sha256','context_sha256'] as $col)if(!isset($cols[$col]))throw new RuntimeException('PARTIAL_RICH_SCHEMA');
 $r=engine::mysql("SELECT COUNT(*) n FROM information_schema.triggers WHERE trigger_schema='brain' AND trigger_name='vlad_m100_rich_insert_guard'");
 if((int)mysqli_fetch_assoc($r)['n']===0)engine::mysql("CREATE TRIGGER vlad_m100_rich_insert_guard BEFORE INSERT ON $table FOR EACH ROW BEGIN IF NEW.dataset_version='model100-h1-v2full-20261109' AND (NEW.rich_context IS NULL OR NEW.rich_sha256 IS NULL OR NEW.context_sha256 IS NULL OR JSON_TYPE(NEW.rich_context)<>'OBJECT') THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='MODEL100_RICH_PAYLOAD_REQUIRED'; END IF; END");
 $reg=json_decode(file_get_contents($base.'/registration.json'),true,512,JSON_THROW_ON_ERROR);
 foreach($reg as $mode=>$s){
  $v=[];foreach([$s['dataset_version'],$mode,$s['schema_hash'],$s['schema_json'],$s['builder_code_sha256'],$s['builder_version']] as $x)$v[]="'".engine::escape_string($x)."'";
  $r=engine::mysql("SELECT schema_hash,builder_code_sha256 FROM vlad_model100_h1_feature_sets WHERE dataset_version='".engine::escape_string($s['dataset_version'])."' AND mode='".engine::escape_string($mode)."'");
  if($old=mysqli_fetch_assoc($r)){if($old['schema_hash']!==$s['schema_hash']||$old['builder_code_sha256']!==$s['builder_code_sha256'])throw new RuntimeException('V2_REGISTRATION_CONFLICT');}
  else engine::mysql('INSERT INTO vlad_model100_h1_feature_sets(dataset_version,mode,schema_hash,schema_json,builder_code_sha256,builder_version) VALUES('.implode(',',$v).')');
 }
 echo 'RICH_COLUMNS_AND_SCHEMAS_READY_V1_ROWS_UNMODIFIED'.PHP_EOL;
}finally{BrainParserWriterFence::release($lease);}
