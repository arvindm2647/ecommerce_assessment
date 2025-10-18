import os
import asyncio
from datetime import datetime
from typing import Dict, Optional, Any
import json

class Config:
    def __init__(self):
        self.logger = None
        self.sleep = asyncio.sleep
    
    def get_constructed(self, name):
        return self.logger
    
    def log_msg(self, level, message):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] [{level.upper()}] {message}")

config = Config()
logger = config

class Logger:
    def __init__(self):
        self.task_id = None
    
    def info(self, message):
        config.log_msg('info', message)
    
    def unexpected_err(self, err):
        config.log_msg('error', f"Unexpected error: {str(err)}")

log_mgr = Logger()

class MongoManager:
    def __init__(self):
        self.db_cnx = None
        self.db_name = "file_processor_db"
    
    async def get_db_cnx(self):
        self.db_cnx = {"connected": True}
        return self.db_cnx
    
    def get_db_name(self, cnx):
        return self.db_name

mongo_mgr = MongoManager()

class FilesManager:
    def __init__(self):
        self.file_queue = []
    
    async def init(self):
        log_mgr.info("Files Manager initialized")
    
    async def get_first_available_file(self, files_path, reverse_time_order=False):
        repo_path = files_path.get('repo_path')
        if not repo_path or not os.path.exists(repo_path):
            return None
        
        files = [f for f in os.listdir(repo_path) if os.path.isfile(os.path.join(repo_path, f))]
        
        if not files:
            return None
        
        files.sort(reverse=reverse_time_order)
        
        return {
            'file_name': files[0],
            'full_path': os.path.join(repo_path, files[0])
        }
    
    def move_file(self, file_name, from_path, to_path):
        try:
            src = os.path.join(from_path, file_name)
            dst = os.path.join(to_path, file_name)
            
            os.makedirs(to_path, exist_ok=True)
            
            if os.path.exists(src):
                os.rename(src, dst)
                return True
            return False
        except Exception as e:
            log_mgr.unexpected_err(e)
            return False
    
    async def clean_file_process_q(self, file_name, error=None):
        if error:
            log_mgr.info(f"Cleaning queue with error for file: {file_name}")
        else:
            log_mgr.info(f"Successfully cleaned queue for file: {file_name}")

files_mgr = FilesManager()

class Commons:
    class OnFileProcErr:
        FAIL = 'FAIL'
        OK = 'OK'

commons_lib = Commons()
on_file_proc_err = commons_lib.OnFileProcErr()

class MongoCommonCollections:
    def __init__(self):
        self.jobs = {}
        self.sequences = {}
        self.job_counter = 0
    
    async def create_job(self, payload):
        self.job_counter += 1
        job_id = f"JOB_{self.job_counter}"
        self.jobs[job_id] = payload
        log_mgr.info(f"Job created with ID: {job_id}")
        return job_id
    
    async def update_job(self, job_id, payload):
        if job_id in self.jobs:
            self.jobs[job_id].update(payload)
            log_mgr.info(f"Job updated: {job_id}")
        return True
    
    async def get_sequence(self, name):
        if name not in self.sequences:
            self.sequences[name] = 0
        self.sequences[name] += 1
        return self.sequences[name]

mongo_common_collections = MongoCommonCollections()

class FileParser:
    async def init(self):
        log_mgr.info("File Parser initialized")
    
    async def import_feed_file(self, file_to_proc, job_id):
        file_name = file_to_proc.get('file_name')
        log_mgr.info(f"Importing feed file: {file_name}")
        
        try:
            summary = {
                'totals': {
                    'raw_lines': 100,
                    'records_ok': 95,
                    'transactions': 95,
                    'site_name': 'Site_A'
                },
                'error': None
            }
            
            await asyncio.sleep(0.1)
            
            return summary
        except Exception as e:
            return {
                'totals': {
                    'raw_lines': 0,
                    'records_ok': 0,
                    'transactions': 0
                },
                'error': {'flag': on_file_proc_err.FAIL, 'message': str(e)}
            }

file_parser = FileParser()

local_db_cnx = None
proc_files_h = None
my_proc_name = "FileProcessor"

async def init():
    global local_db_cnx, proc_files_h
    
    try:
        local_db_cnx = await mongo_mgr.get_db_cnx()
        log_mgr.info(f"Connected OK to mongo database ==> {mongo_mgr.get_db_name(local_db_cnx)}")
        proc_files_h = {"collection": "processedFilesH"}
        await files_mgr.init()
        await file_parser.init()
    except Exception as err:
        log_mgr.unexpected_err(err)
        raise err

async def process_next_file(files_path, reverse_time_order=False):
    local_err = None
    file_to_proc = None
    process_results_doc = None
    
    try:
        try:
            file_to_proc = await files_mgr.get_first_available_file(files_path, reverse_time_order)
            
            if not file_to_proc:
                return None
            
            start_proc_file_time = datetime.now()
            log_mgr.info(f"###### File Process Start : ========> : {start_proc_file_time}")
            log_mgr.info(f"Processing file : {file_to_proc['file_name']}")
            
            proc_name = 'input_processor'
            job_payload = {
                'process_id': my_proc_name,
                'file': file_to_proc['file_name'],
                'star_date': start_proc_file_time.isoformat(),
                proc_name: {
                    'start_time': start_proc_file_time.isoformat(),
                    'status': 'processing'
                }
            }
            job_id = await mongo_common_collections.create_job(job_payload)
            log_mgr.task_id = job_id
            
            summary = await file_parser.import_feed_file(file_to_proc, job_id)
            
            ret = summary.get('error', {}).get('flag') if summary.get('error') else 'OK'
            to_path = files_path.get('err_path') if ret == on_file_proc_err.FAIL else files_path.get('ok_path')
            
            files_mgr.move_file(file_to_proc['file_name'], files_path.get('repo_path'), to_path)
            log_mgr.info('File moved Successfully')
            
            if summary['totals'].get('site_name'):
                job_payload[proc_name]['site_name'] = summary['totals']['site_name']
                del summary['totals']['site_name']
            
            process_results_doc = create_process_doc(start_proc_file_time, summary)
            
            log_mgr.info(f"Process finished for file : {file_to_proc['file_name']} -- Summary : {json.dumps(summary)}")
            
            job_payload[proc_name]['status'] = 'error' if ret == on_file_proc_err.FAIL else 'finished'
            summ_src = job_payload[proc_name]['summary'] = dict(process_results_doc)
            
            mv_out_arr = [
                'start_time',
                'end_time',
                'elapsed_time_in_millisecs',
                'elapsed_time_in_secs',
                'valid_transac_perc',
                'tps'
            ]
            
            for key in mv_out_arr:
                if key in summ_src:
                    job_payload[proc_name][key] = summ_src[key]
                    del summ_src[key]
            
            await mongo_common_collections.update_job(job_id, job_payload)
            
            log_mgr.task_id = None
            
            return {'file_name': file_to_proc['file_name'], 'hist_doc': process_results_doc}
            
        except Exception as err:
            log_mgr.unexpected_err(err)
            local_err = err
        finally:
            if file_to_proc:
                await files_mgr.clean_file_process_q(file_to_proc['file_name'], local_err)
                log_mgr.info('Pending Files Q - FINAL update :  SUCCESSFUL')
        
        if not file_to_proc:
            return None
        
        return {'file_name': file_to_proc['file_name'], 'hist_doc': process_results_doc}
        
    except Exception as err:
        log_mgr.unexpected_err(err)
        raise err

def create_process_doc(start_proc_file_time, summary):
    try:
        doc = {}
        tot = summary['totals']
        doc['start_time'] = start_proc_file_time.isoformat()
        doc['end_time'] = datetime.now().isoformat()
        
        end_time = datetime.fromisoformat(doc['end_time'])
        doc['elapsed_time_in_millisecs'] = int((end_time - start_proc_file_time).total_seconds() * 1000)
        doc['elapsed_time_in_secs'] = doc['elapsed_time_in_millisecs'] / 1000
        doc['valid_transac_perc'] = (tot['records_ok'] / (tot.get('raw_lines') or 1)) * 100
        doc['tps'] = (tot.get('transactions') or -1) / (doc['elapsed_time_in_secs'] or 1)
        
        for key in summary:
            if isinstance(summary[key], dict):
                doc[key] = summary[key]
        
        return doc
    except Exception as err:
        log_mgr.unexpected_err(err)
        raise err

async def update_history(doc):
    try:
        log_mgr.info(f"Inserting document to history: {json.dumps(doc, default=str)}")
    except Exception as err:
        log_mgr.unexpected_err(err)
        raise err

async def main():
    try:
        await init()
        
        files_path = {
            'repo_path': './input_files',
            'ok_path': './processed_files',
            'err_path': './error_files'
        }
        
        os.makedirs(files_path['repo_path'], exist_ok=True)
        os.makedirs(files_path['ok_path'], exist_ok=True)
        os.makedirs(files_path['err_path'], exist_ok=True)
        
        result = await process_next_file(files_path, reverse_time_order=False)
        
        if result:
            log_mgr.info(f"Processing complete: {json.dumps(result, default=str)}")
        else:
            log_mgr.info("No files to process")
            
    except Exception as e:
        log_mgr.unexpected_err(e)

if __name__ == "__main__":
    asyncio.run(main())