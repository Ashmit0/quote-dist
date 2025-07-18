# Quoting Order Distribution  

### Virtual Env

Once cloned first create virtual env. To do this in the local directory
```bash
conda create <env_name> python=3.10
conda activate <env_name>
```

Now install the required libraries
```bash
conda install numpy pandas pyyaml tqdm sortedcontainers IPython 
pip install py_vollib_vectorized
pip install -e .
```

### User Input 

fill the `user_input.yaml` appropriately. 

- `chunk_size` : the number of lines to read the obo data in one step. 
- `date` (Optional): the date to run the script for. 

- `near` , `far` and `farfar` : set the near far and farfar future symbols. 

- `underlying` (Optional) : the underlying in question. 


- `contract_dir` : path to the contract master. 

- `t1` : the time (in nano)  a near book update must stay for it to be considered as a valid update. If a update is validated then no quoting order is permetted to skip this near book update, otherwise the quoting orders are permitted to skip a update if it is disqualifies, i.e there was a new near side update within the t1 time frame. 

- `alpha` : user set hyperparameter, `alpha` * `t1` is the time within which a quoting order must update its order corresponding to the near side update. 

- `mu` : the buffer time to consider a disqualified near book update into accout after it is disregarded. Also in nanos. 


- `T` : Time in nanos after which to show the restuls. If set to 60 seeconds, the number of quoting orders will be displayed in the result csv every minute of the trading day. 

- `beta` : the permissiable percentage change in spread price allowed per update with respect to the origional spread price. used when matching cancled and new orders. 

- `gamma` : same as `beta`. used when hoping from one update to another with no acctual change in the inderstated quoting order. 


- `theta` : The minimum number of times an order should be updated according to the changes in near book to be considered a _definate_ quoting order. 

- `max_level` : The max level within which a order on the far to be considered a quoting order. 

- `result_path` : path to store the result csv named `{underlying}_{date}_{result}.csv`. 

- `log_path` : path to store the log file `{underlying}_{date}_log`. 


### Usage 

The script is designed to run for a single underlying on a single date on the obo data. 

To run it fill the `user_input.yaml` appropriately and then run ( in underlying and date are provided in the inputs ) : 
```bash 
python script.py 
``` 

If one wishes to provide the inputs from the terminal use : 
```bash 
python script.py {underlying} {date}
```

example : 
```bash 
python script.py HFCL 20250519
```

Note : all dates are in 'YYYYMMDD' format. 

Note : In case both terminal inputs and yaml inputs are provided, the script will run on the inputs provided in the terminal. 

To run the script for multiple underlying stockes on multiple dates a sample `run_all.sh` bash script is provided. 

