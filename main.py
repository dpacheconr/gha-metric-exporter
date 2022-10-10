import logging
import schedule
import datetime
import requests
import json
import os
import random
from configuration import get_configuration as conf
import time
from multiprocessing import Process

package_uuid = os.getenv('NEWRELIC_PACKAGE_UUID') 
document_id = os.getenv('NEWRELIC_DOCUMENT_ID') 
account_id = os.getenv('NEWRELIC_ACCOUNT_ID')
newrelic_api_key = os.getenv('NEWRELIC_API_KEY')
newrelic_user_key = os.getenv('NEWRELIC_USER_KEY') 

class loadgen:
    def check_env_vars():
        keys = ("NEWRELIC_PACKAGE_UUID","NEWRELIC_DOCUMENT_ID","NEWRELIC_ACCOUNT_ID","NEWRELIC_API_KEY","NEWRELIC_USER_KEY")
        keys_not_set = []

        for key in keys:
            if key not in os.environ:
                keys_not_set.append(key)
        else:
            pass

        if len(keys_not_set) > 0: 
            for key in keys_not_set:
                print(key + " not set")
            exit(1)
        else:
            print("All environment variables set, starting load generator.")

    def grab_conf():
        loadgen.check_env_vars()
        config = conf.get_config(newrelic_user_key,package_uuid,account_id,document_id)
        print("Configuration received from Synthetic Configurator: " + str(config))
        return config

    def send_to_nr_batch_data(config): 
        timestamp = str(int(round(datetime.datetime.timestamp(datetime.datetime.now()))))
        batch = (int(config['duration']) // int(config['frequency']))
        batch_count = (int(config['timeout']) // batch)
        url = config['metrics_url']
        headers = {'Content-Type': 'application/json', 'Api-Key': newrelic_api_key}
        attributes_str = ",\"attributes\":{\"batch.count\":\""+str(batch_count)+"\"}}" 
        metrics = "{\"name\":\"loadgenerator.metrics\",\"type\":\""+config['metric_type']+"\",\"value\":"+str(config['timeout'])+",\"timestamp\":"
        metrics_to_send = "[{\"metrics\":["+metrics+timestamp+attributes_str+"]}]"
        response = requests.post(url, headers=headers, json=json.loads(metrics_to_send))
        print(str(config['metric_attribute_name']) + " - Batch metrics sent to NR with status code: " + str(response.status_code) + " and requestId " + str(response.json()['requestId']))
        return response.json()
    
    def send_to_nr(metrics,config):  
        url = config['metrics_url']
        headers = {'Content-Type': 'application/json', 'Api-Key': newrelic_api_key}
        response = requests.post(url, headers=headers, json=json.loads(metrics))
        print(config['metric_attribute_name']+ " - Batch data sent to NR with status code: " + str(response.status_code) + " and requestId " + str(response.json()['requestId']))
        loadgen.send_to_nr_batch_data(config)
        return response.json()

    def metrics_str (config):
        low_val_str = "{\"name\":\""+config['metric_name']+"\",\"type\":\""+config['metric_type']+"\",\"value\":"+config['low_val']+",\"timestamp\":"
        high_val_str = "{\"name\":\""+config['metric_name']+"\",\"type\":\""+config['metric_type']+"\",\"value\":"+config['high_val']+",\"timestamp\":"
        attributes_str = ",\"attributes\":{\""+config['metric_attribute_name']+"\":\""+config['metric_attribute_value']+"\"}}," 
        return low_val_str,high_val_str,attributes_str
         

    def timestamp_str(n,batch):
        current_time = datetime.datetime.now()
        timestamp = str(int(round(datetime.datetime.timestamp(current_time - datetime.timedelta(seconds=int(n)*int(batch))))))
        return timestamp

    def construct_data_to_add(n,batch,i,config):
        construct_data_to_add = loadgen.metrics_str(config)[i] + loadgen.timestamp_str(n,batch) + loadgen.metrics_str(config)[2]
        return construct_data_to_add

    def job(config):
        batch = (int(config['duration']) // int(config['frequency']))
        batch_count = (int(config['timeout']) // batch)
        if batch_count == 0:
            print(config['metric_attribute_name']+ " - Your send batches value is incorrect, please review your configuration")
            exit(0)
        construct_data_to_add = ""
        n = 0
        i = 1
        spike_at = []
        while i < batch_count+1:
            if int(config['const_spike']) != 0:
                print(str(config['metric_attribute_name'])+" - Constant spiking every " + str(config['const_spike'] + " seconds" ))
                const_spike_count = (int(config['timeout']) // int(config['const_spike']))
                for z in range(const_spike_count):
                    z += 1
                    spike_at.append(z*config['const_spike'])
                for n in range(1,batch_count+1):              
                    if n * batch not in spike_at:
                        construct_data_to_add += loadgen.construct_data_to_add(n,batch,0,config)
                    else:
                        pass
                for n in spike_at:
                    construct_data_to_add += loadgen.construct_data_to_add(n,batch,1,config)
                break
            elif config['random_spike']:
                print(str(config['metric_attribute_name'])+" - Random spiking")
                for n in range(1,batch_count+1):
                    if n == n*random.randint(1,3):
                        construct_data_to_add += loadgen.construct_data_to_add(n,batch,1,config)
                    else:
                        construct_data_to_add += loadgen.construct_data_to_add(n,batch,0,config)
                break
            else:
                print(str(config['metric_attribute_name']) + " - Spike from minute x to minute y")
                if (i * batch) in range(config['spike_from'],config['spike_to']+60):
                    n = i
                    construct_data_to_add += loadgen.construct_data_to_add(n,batch,1,config)
                    i += 1
                else: 
                    n = i
                    construct_data_to_add += loadgen.construct_data_to_add(n,batch,0,config)
                    i += 1


        metrics = "[{\"metrics\":["+construct_data_to_add[:-1]+"]}]"
        return loadgen.send_to_nr(metrics,config)

    def run_app(config):
        run_x_amount_times = int(config['run_x_amount_times'])
        timeout = int(config['timeout'])
        if run_x_amount_times != 0:
            schedule.every(timeout).seconds.do(loadgen.job, config=config).until(datetime.timedelta(seconds=(run_x_amount_times+0.1)*timeout))
            
        else:
            schedule.every(timeout).seconds.do(loadgen.job, config=config)
        while True:
            n = schedule.idle_seconds()
            if n is None:
                # no more jobs
                break
            elif n > 0:
                print(str(config['metric_attribute_name']) + " - Gathering data to send, will send a batch in " + str(round(float(str(n)))) + " seconds")
                time.sleep(n+0.1)
            schedule.run_pending()

if __name__ == "__main__":
    configurations = loadgen.grab_conf() 
    for i in range(len(configurations)):
        config = configurations[i]
        process = Process(target=loadgen.run_app, args=(config,))
        process.start()
        time.sleep(1)

  



        
