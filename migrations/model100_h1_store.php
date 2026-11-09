<?php
/** Root-owned migration/control path, existing Brain PHP engine and runner. */
if(PHP_SAPI!=='cli'||!preg_match('/^Uid:\s+0\s+0\s/m',file_get_contents('/proc/self/status')))throw new RuntimeException('ROOT_CLI_REQUIRED');
chdir('/brain/Brain-Server/public_html');
$_SERVER['DOCUMENT_ROOT']=getcwd();$_SERVER['HTTP_HOST']='localhost';$_SERVER['REQUEST_URI']='/model100-feature-store';
$_SERVER['SCRIPT_NAME']='/index.php';$_SERVER['SCRIPT_FILENAME']=getcwd().'/index.php';$_SERVER['EXE']=false;
define('DEBUG',1);define('DEEP_DEBUG',0);
require_once 'engine/nodes/config.php';require_once 'engine/nodes/remote_compute_config.php';
require_once 'engine/core/engine.php';require_once 'engine/nodes/core_autoload.php';
require_once 'engine/core/BrainParserWriterFence.php';require_once 'engine/core/BrainParserHealthMonitor.php';
if(($_SERVER['config']['node_name']??'')!=='Brain 1')throw new RuntimeException('B1_ONLY');
$base='/brain/model100-featurebuilder-20261109/mysql-store';$action=$argv[1]??'dry-run';
$table='vlad_model100_h1_features';$script='model100_feature_store_parser.py';
$q=engine::mysql("SELECT id,script,`table`,argument,date FROM brain_parser WHERE `table`='$table' OR script='$script'");
$rows=[];while($r=mysqli_fetch_assoc($q))$rows[]=$r;
if(count($rows)>1)throw new RuntimeException('AMBIGUOUS_REGISTRY');
if($action==='deploy'){
 $target=$argv[2]??'';if(!preg_match('/^[0-9a-f]{40}$/D',$target))throw new RuntimeException('COMMIT_REQUIRED');
 $lease=BrainParserWriterFence::acquireForRegisteredTable($table,true);if($lease===false)throw new RuntimeException('WRITER_BUSY');
 try{
 $repo='/brain/Brain-Services';$head=trim(shell_exec('git -C '.escapeshellarg($repo).' rev-parse HEAD'));
 exec('git -C '.escapeshellarg($repo).' merge-base --is-ancestor '.escapeshellarg($head).' '.escapeshellarg($target),$out,$rc);
 if($rc!==0)throw new RuntimeException('FAST_FORWARD_ONLY');
 $changed=trim(shell_exec('git -C '.escapeshellarg($repo).' diff --name-only '.escapeshellarg($head).' '.escapeshellarg($target)));
 foreach(explode("\n",$changed) as $path)if($path!==''&&!preg_match('~^(shared/model100_|parser/model100_|tests/test_model100|migrations/model100_|tools/model100_|docs/)~D',$path))throw new RuntimeException('UNEXPECTED_PRODUCTION_DIFF:'.$path);
 $configBefore=hash_file('sha256',$repo.'/94/config.toml');$entry=$repo.'/parser/'.$script;
 if(str_contains($changed,'parser/'.$script)===false&&$head!==$target){
  // Library-only fixes are safe when the existing tracked entrypoint is exactly
  // the target Git blob. Never accept a stale symlink or local replacement.
  $expected=trim(shell_exec('git -C '.escapeshellarg($repo).' rev-parse '.escapeshellarg($target.':parser/'.$script)));
  $actual=trim(shell_exec('git -C '.escapeshellarg($repo).' hash-object '.escapeshellarg($entry)));
  if(is_link($entry)||!preg_match('/^[0-9a-f]{40}$/D',$expected)||!hash_equals($expected,$actual))throw new RuntimeException('ENTRYPOINT_SOURCE_MISMATCH');
 }
 $backup=$base.'/entrypoint-before-deploy.symlink';
 if(is_link($entry)){if(file_exists($backup)||is_link($backup))throw new RuntimeException('DEPLOY_BACKUP_EXISTS');rename($entry,$backup);}
 exec('git -C '.escapeshellarg($repo).' merge --ff-only '.escapeshellarg($target).' 2>&1',$output,$rc);
 if($rc!==0){if(!file_exists($entry)&&is_link($backup))rename($backup,$entry);throw new RuntimeException('DEPLOY_FF_FAILED:'.implode(' ',array_slice($output,-3)));}
 if(trim(shell_exec('git -C '.escapeshellarg($repo).' rev-parse HEAD'))!==$target||hash_file('sha256',$repo.'/94/config.toml')!==$configBefore)throw new RuntimeException('DEPLOY_POSTCONDITION');
 echo json_encode(['head_before'=>$head,'head_after'=>$target,'model94_config_unchanged'=>true,'restarts'=>0]).PHP_EOL;
 }finally{BrainParserWriterFence::release($lease);}exit;
}
if($action==='archive-json-trial'){
 if(count($rows)!==1)throw new RuntimeException('REGISTRATION_REQUIRED');
 $backup=$base.'/json-numeric-trial-before-rename.json';
 if(file_exists($backup))throw new RuntimeException('TRIAL_ARCHIVE_ALREADY_ATTEMPTED');
 $lease=BrainParserWriterFence::acquireForRegisteredTable($table,true);if($lease===false)throw new RuntimeException('WRITER_BUSY');
 try{
 engine::mysql("UPDATE brain_parser SET date='2999-01-01' WHERE id=".(int)$rows[0]['id']);
 $counts=[];$r=engine::mysql("SELECT mode,COUNT(*) n FROM $table GROUP BY mode");while($v=mysqli_fetch_assoc($r))$counts[]=$v;
 file_put_contents($backup,json_encode(['reason'=>'MYSQL_JSON_NUMERIC_ROUNDTRIP_ULP_LOSS','rows'=>$counts,'registration'=>$rows],JSON_PRETTY_PRINT));chmod($backup,0600);
 engine::mysql('RENAME TABLE vlad_model100_h1_features TO vlad_model100_h1_features_json_trial, vlad_model100_h1_feature_sets TO vlad_model100_h1_sets_json_trial');
 echo 'TRIAL_PRESERVED_AT_SEPARATE_TABLES'.PHP_EOL;
 }finally{BrainParserWriterFence::release($lease);}exit;
}
if(in_array($action,['import','normal'],true)){
 if(count($rows)!==1||$rows[0]['script']!==$script||$rows[0]['table']!==$table)throw new RuntimeException('REGISTRATION_REQUIRED');
 $src=file_get_contents('engine/code/parser.php');$a=strpos($src,'function parser_run_registered_row(');$b=strpos($src,'function parser()');
 if($a===false||$b===false)throw new RuntimeException('NATIVE_RUNNER_MISSING');
 eval(substr($src,$a,$b-$a));$row=$rows[0];$row['argument']=$action;
 echo json_encode(['native_runner'=>parser_run_registered_row($row),'parser_id'=>(int)$row['id']]).PHP_EOL;exit;
}
if($action==='activate'){
 if(count($rows)!==1)throw new RuntimeException('REGISTRATION_REQUIRED');
 $r=engine::mysql("SELECT mode,COUNT(*) n FROM $table GROUP BY mode");$n=0;while($v=mysqli_fetch_assoc($r)){if((int)$v['n']<11443)throw new RuntimeException('IMPORT_INCOMPLETE');$n++;}
 if($n!==2)throw new RuntimeException('BOTH_MODES_REQUIRED');
 engine::mysql("UPDATE brain_parser SET date=NULL WHERE id=".(int)$rows[0]['id']);echo 'ACTIVATED_FOR_NATIVE_CRON'.PHP_EOL;exit;
}
if(!in_array($action,['dry-run','apply'],true))throw new RuntimeException('INVALID_ACTION');
$existing=[];foreach([$table,'vlad_model100_h1_feature_sets'] as $t){$r=engine::mysql("SELECT COUNT(*) n FROM information_schema.tables WHERE table_schema='brain' AND table_name='$t'");$existing[$t]=(int)mysqli_fetch_assoc($r)['n'];}
echo json_encode(['mode'=>$action,'tables_before'=>$existing,'registration_before'=>$rows,'replication'=>'NONE_B1_ONLY']).PHP_EOL;
if($action==='dry-run')exit;
umask(0007);if(!is_dir($base))mkdir($base,0770,true);chgrp($base,48);chmod($base,02770);
if(!file_exists($base.'/before.json')){file_put_contents($base.'/before.json',json_encode(['tables'=>$existing,'registration'=>$rows],JSON_PRETTY_PRINT));chmod($base.'/before.json',0600);}
$py="from pathlib import Path; import sys; sys.path.insert(0,'/brain/Brain-Server/install/lib'); from source_writer_fence import provision_parser_lock_tables; provision_parser_lock_tables(Path('/'),['$table'])";
exec('python3 -c '.escapeshellarg($py),$provision,$rc);if($rc!==0)throw new RuntimeException('NATIVE_LOCK_PROVISION_FAILED');
$boot="d /run/lock/brain-native-parser-writers 0750 root apache -\nf /run/lock/brain-native-parser-writers/".hash('sha256',$table).".lock 0660 root apache -\n";
$bootPath='/etc/tmpfiles.d/brain-model100-feature-store.conf';
if(file_exists($bootPath)&&file_get_contents($bootPath)!==$boot)throw new RuntimeException('BOOT_LOCK_CONFIG_CONFLICT');
if(!file_exists($bootPath)){file_put_contents($bootPath,$boot);chmod($bootPath,0644);}
$ddl=file_get_contents(__DIR__.'/model100_h1_store.sql');foreach(explode(";\n",$ddl) as $statement)if(trim($statement))engine::mysql(trim($statement));
foreach(['UPDATE'=>'vlad_m100_hex_no_update','DELETE'=>'vlad_m100_hex_no_delete'] as $op=>$name){
 $r=engine::mysql("SELECT COUNT(*) n FROM information_schema.triggers WHERE trigger_schema='brain' AND trigger_name='$name'");
 if((int)mysqli_fetch_assoc($r)['n']===0)engine::mysql("CREATE TRIGGER $name BEFORE $op ON $table FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='MODEL100_IMMUTABLE_HISTORY'");
}
$registration=json_decode(file_get_contents($base.'/schema-registration.json'),true,512,JSON_THROW_ON_ERROR);
foreach($registration as $mode=>$s){
 $vals=[];foreach([$s['dataset_version'],$mode,$s['schema_hash'],$s['schema_json'],$s['builder_code_sha256'],$s['builder_version']] as $v)$vals[]="'".engine::escape_string($v)."'";
 $r=engine::mysql("SELECT schema_hash,builder_code_sha256 FROM vlad_model100_h1_feature_sets WHERE dataset_version='".engine::escape_string($s['dataset_version'])."' AND mode='".engine::escape_string($mode)."'");
 if($old=mysqli_fetch_assoc($r)){if($old['schema_hash']!==$s['schema_hash']||$old['builder_code_sha256']!==$s['builder_code_sha256'])throw new RuntimeException('EXISTING_SCHEMA_CONFLICT');}
 else engine::mysql('INSERT INTO vlad_model100_h1_feature_sets(dataset_version,mode,schema_hash,schema_json,builder_code_sha256,builder_version) VALUES('.implode(',',$vals).')');
}
if(!$rows){engine::mysql("INSERT INTO brain_parser(user_id,script,`table`,argument,`interval`,date_field,modify,date) VALUES(0,'$script','$table','normal',0,'cutoff_utc',0,'2999-01-01')");$id=mysqli_insert_id($_SERVER['sql_connection']);}
else{$id=(int)$rows[0]['id'];if($rows[0]['script']!==$script)throw new RuntimeException('REGISTRY_CONFLICT');}
foreach([$table,'vlad_model100_h1_feature_sets'] as $t){
 $r=engine::mysql("SELECT parser_id,replication_direction FROM brain_parser_objects WHERE object_schema='brain' AND object_name='$t'");
 if($v=mysqli_fetch_assoc($r)){if((int)$v['parser_id']!==$id||$v['replication_direction']!=='NONE')throw new RuntimeException('OWNERSHIP_CONFLICT');}
 else engine::mysql("INSERT INTO brain_parser_objects(parser_id,object_schema,object_name,object_type,ownership_role,source_node,replication_direction,required_children,enabled) VALUES($id,'brain','$t','BASE TABLE','CANONICAL','brain1','NONE',JSON_ARRAY(),1)");
}
echo json_encode(['applied'=>true,'parser_id'=>$id,'historical_rows_modified'=>0]).PHP_EOL;
