import os 
import sys 
import csv 
import yaml
import pickle 
import numpy as np 
import pandas as pd 
from tqdm import tqdm
import datetime 


# define fixed variable and user inputs : 

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
 
header_list = [
'trading_date','data_source','stream_id','exchange_epoch_nanos','server_epoch_nanos','capture_epoch_nanos',
'contract_id','venue_token','contract_name','market_segment_id','symbol','contract_type','strike_price','expiry date',
'display_factor','handle_value','currency_unit','is_touchline_change','feed_message_id','inpacket_sequence_num',
'is_last_in_feed_message','event_type','packet_sequence_num','contract_sequence_num','is_implied','transact_epoch_nanos',
'bp1','bq1','boc1','has_hidden_qty_bid','ap1','aq1','aoc1','has_hidden_qty_ask','bp2','bq2','boc2','ap2','aq2','aoc2',
'bp3','bq3','boc3','ap3','aq3','aoc3','bp4','bq4','boc4','ap4','aq4','aoc4','bp5','bq5','boc5','ap5','aq5','aoc5',
'implied_bp1','implied_bq1','implied_ap1','implied_aq1','implied_bp2','implied_bq2','implied_ap2','implied_aq2',
'previous_mid','mid','is_mid_change','weighted_mid','contract_status','side','price','qty','order_count','qpos',
'old_qpos','oid1','oid2','priority','old_price','old_qty','aggressor_type','qty_in_last_trade','level','capture_ts timestamptz'
]

header_list_to_keep = [
'exchange_epoch_nanos','contract_name','expiry date','is_touchline_change','event_type',
'bp1','bq1','boc1','ap1','aq1','aoc1','bp2','bq2','boc2','ap2','aq2','aoc2',
'bp3','bq3','boc3','ap3','aq3','aoc3','bp4','bq4','boc4','ap4','aq4','aoc4','bp5','bq5','boc5','ap5','aq5','aoc5',
'previous_mid','mid','is_mid_change','weighted_mid','side','price','qty','qpos',
'old_qpos','oid1','oid2','old_price','old_qty','aggressor_type','qty_in_last_trade','level'
]

cols_to_check = [
'market_segment_id','contract_type',
'display_factor','handle_value','is_touchline_change','inpacket_sequence_num',
'is_last_in_feed_message','contract_sequence_num','is_implied','contract_status','qpos',
'old_qpos'
]

qty_cols = [
    'bq1','aq1','bq2','aq2','bq3','aq3',
    'bq4','aq4','bq5','aq5','qty',
    'old_qty','qty_in_last_trade'
]


book_cols = [
    'bp1','bq1','boc1','ap1','aq1','aoc1','bp2','bq2','boc2',
    'ap2','aq2','aoc2','bp3','bq3','boc3','ap3','aq3','aoc3','bp4','bq4',
    'boc4','ap4','aq4','aoc4','bp5','bq5','boc5','ap5','aq5','aoc5'
]

ask_cols = [
    'ap1','aq1','aoc1',
    'ap2','aq2','aoc2','ap3','aq3','aoc3',
    'ap4','aq4','aoc4','ap5','aq5','aoc5'
]

bid_cols = [
    'bp1','bq1','boc1','bp2','bq2','boc2',
    'bp3','bq3','boc3','bp4','bq4',
    'boc4','bp5','bq5','boc5'
]



ns_cols = ['exchange_epoch_nanos' , 'server_epoch_nanos' , 'capture_epoch_nanos' , 'transact_epoch_nanos' , 'priority']

with open('user_input.yaml' , 'r' ) as f : 
    inputs = yaml.safe_load( f ) 

inputs['underlying'] = sys.argv[1]
inputs['date'] = sys.argv[2]


usecols = [header_list.index(col) for col in header_list_to_keep ]


# path to obo file :
date_dir =  os.path.join(inputs['parent_dir'] , inputs['date'] )
for file in os.listdir( date_dir ) : 
    if file.startswith('obo') : 
        file_path = os.path.join( date_dir , file )
        break 


def get_iter() : 
    return pd.read_csv(file_path, header=None, names=header_list_to_keep , chunksize=inputs['chunk_size']  , dtype = {82 : str} , usecols= usecols ) 


def get_unique( cols_to_check ): 
    check_dict = { col : []  for col in cols_to_check }
    for chunk in get_iter() : 
        for col in cols_to_check : 
            temp = chunk.dropna(subset=[col])
            check_dict[col] = check_dict[col] + temp[col].unique().tolist()
    for col in check_dict : 
        print(f'for {col} : {np.unique(check_dict[col])}')


def convert_ns_to_time(time_ns ) : 
    dt = datetime.datetime.fromtimestamp( np.floor(time_ns/1e9) , tz=IST).replace(tzinfo=None).time()
    ns_part = f"{time_ns%int(1e9)*1e-9:.9f}"

    return dt.strftime('%H:%M:%S') + ns_part[1:]


# get_unique(['event_type' , 'qpos' , 'level'])
# get_unique(['order_count' , 'has_hidden_qty_bid' , 'aggressor_type'])
# get_unique(['order_count'])


def get_lot_size_path(inputs) : 
    return os.path.join( inputs['contract_dir'], inputs['near'] + '_NSE_FO_FUT_LOT_SIZE.pickel' )

def get_contract_master_path(inputs) : 
    year_month_str = pd.to_datetime( inputs['date'] ).strftime(format = '%Y_%m')
    contract_path = [ os.path.join(inputs['contract_dir'], f) for f in os.listdir(inputs['contract_dir']) if f.startswith(year_month_str) and os.path.isfile(os.path.join(inputs['contract_dir'], f))]
    if( len(contract_path) == 0 ) : 
        raise ValueError(f"Contrate Path for {inputs['date']} not found.")
    return contract_path[0]

month_to_nse_code = {
    1: 'F',   # January
    2: 'G',   # February
    3: 'H',   # March
    4: 'J',   # April
    5: 'K',   # May
    6: 'M',   # June
    7: 'N',   # July
    8: 'Q',   # August
    9: 'U',   # September
    10: 'V',  # October
    11: 'X',  # November
    12: 'Z'   # December
}

def convert_date_to_code( date ) : 
    if( type(date) != str ): 
        date = str( date )
    date = pd.to_datetime( date )
    return month_to_nse_code[int(date.strftime('%m'))] + date.strftime('%Y')[2:]

def get_lot_size(inputs) : 
    lot_path = get_lot_size_path(inputs) 
    try : 
        with open( lot_path , 'rb' ) as f : 
            df = pickle.load(f)
    except :  
        df = pd.read_csv(
            get_contract_master_path(inputs) , 
            compression='gzip' , 
            usecols = [2,3,7,12,28]
        )
        df = df[df['type'] == 'FUT' ]
        df['tick_size'] /= 100
        df['expiry_date'] = df['expiry_date'].apply(convert_date_to_code)
        df['symbol'] = 'NSEFNO_' + df['symbol'] + '_' + df['expiry_date']
        df.pop('type')
        df.pop('expiry_date')
        df = df.set_index('symbol')
        # lot_dict = df['lotsize'].to_dict()
        with open( lot_path , 'wb' ) as f : 
            pickle.dump( df , f ) 
    return df 


near_symbol = '_'.join(['NSEFNO',inputs['underlying'],inputs['near']]) 
far_symbol = '_'.join(['NSEFNO',inputs['underlying'],inputs['far']])
farfar_symbol = '_'.join(['NSEFNO',inputs['underlying'],inputs['farfar']])
spread_symbol= '_'.join(['NSEFNO' , inputs['underlying'] , 'SP' , inputs['near'] , inputs['far']])


df = get_lot_size(inputs)
df.head()


if( df.loc[near_symbol]['lotsize'] != df.loc[far_symbol]['lotsize']) : 
    raise ValueError(f'Lot size for {near_symbol} and {far_symbol} are not the same ... ')
else : 
    lotsize = df.loc[near_symbol]['lotsize']


if( df.loc[near_symbol]['tick_size'] != df.loc[far_symbol]['tick_size']) : 
    raise ValueError(f'Tick size for {near_symbol} and {far_symbol} are not the same ... ')
else : 
    ticksize = df.loc[near_symbol]['tick_size']
 

# for chunk in get_iter() : 

#     chunk = chunk[(chunk['contract_name'] == near_symbol) | (chunk['contract_name'] == far_symbol ) ].reset_index(drop=True)
#     chunk.insert(0,'Time',chunk['exchange_epoch_nanos'].apply(convert_ns_to_time))
#     # InMarketTime -> time in mili seconds after market starts !! 
#     chunk.insert(0,'InMarketTime' , (((chunk['exchange_epoch_nanos']%(int(24*36*1e11)) - int(13500*1e9)) )).astype(int) )
#     chunk[qty_cols] = (chunk[qty_cols].fillna(0.0)/lotsize).astype(int)
#     chunk[book_cols] = chunk[book_cols].fillna(0.0).astype( float )
#     chunk[['oid1' , 'oid2']] = chunk[['oid1' , 'oid2']].fillna(-1).astype(int)
#     # chunk.fillna('--').to_csv('example_chunk.csv' , index = False )
#     break                 


from collections import deque , Counter 
from sortedcontainers import SortedList , SortedDict

from typing import Optional


# order book according to book ids 
def make_book_dict(book:np.ndarray)->dict: 
    main = {'book' : book}
    main['qty_cumsum'] = np.cumsum(book[: , 1 ] , dtype = int )
    main['total_qty'] = main['qty_cumsum'][-1]
    main['total_def_qty'] = main['qty_cumsum'][-1] - main['qty_cumsum'][0]
    # to be filled later as and when reqired : 
    main['price_cumsum'] = np.zeros(main['total_qty'] , dtype = float ) 
    if main['total_qty'] == 0 : 
        return main
    # fill the price cumsum array :
    main['price_cumsum'][0] = book[0,0]
    j = 0 
    for i in range(2,main['total_qty']+1) :
        if i  > main['qty_cumsum'][j] : 
            j += 1
        main['price_cumsum'][i-1] = main['price_cumsum'][i-2] + book[j,0]
    return main 


# make possible quote entry  :
def make_possible_quote_dict(row,near_ask:dict,near_ask_id:int)->Optional[dict]: 
    book = near_ask[near_ask_id] 
    qty = int(row['qty'])
    top_qty = int(book['book'][0,1])
    if qty > book['total_qty'] : 
        return None 
    main =  {
        'price' : row['price'] , 
        'qty' : qty , 
        'sp1' : round( (row['price'] - book['price_cumsum'][qty-1])/(qty*ticksize))*ticksize ,
        'sp2' : round( (row['price'] - book['price_cumsum'][qty+top_qty-1]\
                        + book['price_cumsum'][top_qty-1])/(qty*ticksize))*ticksize\
                        if qty+top_qty-1 < book['total_qty'] else None , 
        'last_ask_id' : near_ask_id 
    }
    
    main['sp1'] = round( main['sp1'] , 2 ) 
    if  main['sp2'] is not None : 
        main['sp2'] = round( main['sp2'] , 2 ) 
    
    return main 


# make possible quote entry  :
def make_quote_dict(possible_dict:dict,num_updates:int=1)->Optional[dict] : 
    main = possible_dict
    if not main : 
        return main  
    main.pop('sp2' , None ) 
    main['sp'] = main.pop('sp1' , None ) 
    main.pop('last_ask_id')
    main['num_updates'] = num_updates 
    return main 

def make_def_quote_dict(possible_dict:dict,num_updates:int=1)->Optional[dict]: 
    main = possible_dict
    if not main : 
        return main  
    main.pop('sp1' , None ) 
    main['sp'] = main.pop('sp2' , None ) 
    if main['sp'] is None : 
        return None 
    main.pop('last_ask_id')
    main['num_updates'] = num_updates 
    return main 



class Updates_Buffer: 

    def __init__(self) -> None:
        self.expiry =  {}
        self.queue = deque() 

    def not_empty(self)->bool: 
        return len(self.queue)

    def insert(self,id:int,time:int)->None:
        self.expiry[id] = time 
        self.queue.append(id) 
    
    def latest_exp(self)->int: 
        try : 
            return self.expiry[ self.queue[0] ]
        except : 
            raise ValueError('Buffer is Empty')

    def pop(self)->int: 
        try : 
            id = self.queue.popleft() 
            self.expiry.pop(id)
            return id 
        except : 
            raise ValueError('Buffer is Empty')
        
    def clear(self)->None: 
        self.expiry.clear() 
        self.queue.clear()
        
    def __len__(self): 
        return len(self.queue) 
    
    def __iter__(self):
        return iter(self.queue) 
    
    def __getitem__(self,idx:int) : 
        return self.queue[idx]

def list_bisect_left(x:list,q:int)->int: 
    l = 0 
    h = len(x)
    while( l < h ) : 
        m = (l + h) // 2 
        if( x[m] < q ) : 
            l = m + 1 
        else : 
            h = m 
    return l 
    
def list_bisect_right(x:list,q:int)->int: 
    l = 0 
    h = len(x)
    while( l < h ) : 
        m = (l + h) // 2 
        if( x[m] > q ) : 
            h = m  
        else : 
            l = m + 1 
    if ( l < len(x) ) and ( x[l] == q ) : 
        return l + 1 
    return l  

class Sorted_Updates_Buffer:

    def __init__(self) -> None:
        self.expiry =  {}
        self.queue = deque() 
        self.list = [0]

    def not_empty(self)->bool: 
        return len(self.queue)

    def insert(self,id:int,time:int)->None:
        if id in self.expiry :
            raise ValueError(f'Id {id} already exists in the buffer')
        self.expiry[id] = time 
        self.queue.append(id) 
        self.list.append(id)
    
    def latest_exp(self)->int: 
        try : 
            return self.expiry[ self.queue[0] ]
        except : 
            raise ValueError('Buffer is Empty')

    def pop(self)->int: 
        try : 
            id = self.queue.popleft() 
            self.expiry.pop(id)
            return id 
        except : 
            raise ValueError('Buffer is Empty')
        
    def last_verified_id(self,id:int)->int : 
        idx = list_bisect_left(self.list,id)
        try : 
            return self.list[idx-1]
        except : 
            raise ValueError('Wrong input id to check?')
        
    def next_verified_id(self,id:int)->int : 
        idx = list_bisect_right(self.list,id)
        try : 
            return self.list[idx]
        except : 
            return -1 

    def clear(self)->None : 
        self.queue.clear() 
        self.expiry.clear() 
        self.list = [self.list[-1]]
        
           
    def __len__(self): 
        return len(self.queue) 
    
    def __iter__(self):
        return iter(self.queue) 
    
    def __str__(self) -> str:
        return '{:10s}:{}\n{:10s}:{}\n{:10s}:{}'.format(
            'Expity' , self.expiry, 
            'Queue' , self.queue , 
            'List' , self.list 
        )
        
    def __getitem__(self,idx:int): 
        return self.queue[idx]


# ith entry denoted the min price at which one can buy i quantities of Near contracts 
NA_Qty_Price = np.zeros( shape = 50 ) 

# known quoting and defencive quoting orders respectively : 
#   orders yet to be labled as bidding or market : 
possible_quotes = {}
#   orders identified with orderid : 
quote = {}  
def_quote = {}
#   keep cound of sucessful model verification : 
# successful_order_updates = {}

# current near ask id : 
near_ask_id = 0 
current_verified_id = 0

first_near_ask_id_window = 0 

possible_quotes[near_ask_id] = {} 
quote[near_ask_id] = {}
def_quote[near_ask_id] = {}

# key : sp 
# val : total number of orders  
total_quoting_orders = {} 

# key : near ask id ! 
near_ask =  { 0 : make_book_dict(np.zeros((5,3)))}
far_ask  =  { 0 : make_book_dict(np.zeros((5,3)))}

# track of near ask updates that are not expired ( open to sucessulf verification  ) 
# Note : at any given time we can only have at most 1 possible update candidate 
# that, a new update before verification will render the previous one disquallified. 
possible_update_candidate_id = 0
time_to_verification = 0
# update time for verified entries 
verified_buffer = Sorted_Updates_Buffer()
# disqualified updates budffer time : 
disqualified_buffer = Updates_Buffer() 

# time constants : 
t1 , t2 , T , mu = (
    int(inputs['t1'] ), 
    int(inputs['t1'])*int(inputs['alpha']) , 
    int(inputs['T']) , 
    int(inputs['mu'])
)

# beta hyperparameter : 
beta = int(inputs['beta'])

# theta 
theta = int(inputs['theta'])

# max levels : 
max_level = int(inputs['max_level'])

# time in the previous row to detect trade ticks : 
prev_time = -1 


def counter(start:int=0): 
    while True : 
        yield 'dummy' + str(start)
        start += 1 
        

def get_pickel_file_name( inputs , num:int ): 
    return os.path.join( inputs['save_path'] , '_'.join([inputs['underlying'] , inputs['date'] , 'total_quotes' , str(num) + '.pkl' ] ))

order_id_map = {} 
script_order_id_counter = counter()


class OrderContainer : 
    
    def __init__(self) -> None:
        self.list = SortedList() 
        self.count = Counter()
        self.order_id =  {}

    # order -> tuple of ( spread , qty ) 

    def insert(self,order:tuple,order_id)->None: 
        self.count[order] += 1 
        if order in self.order_id : 
            self.order_id[order].append(order_id)
        else : 
            self.order_id[order] = deque([order_id])
            self.list.add(order)

    def exist(self,order:tuple)->bool:
        return self.count[order] 
    
    def remove_order(self,order:tuple)->Optional[tuple]: 
        if not self.exist(order) : 
            return None  
        self.count[order] -= 1 
        id = self.order_id[order].popleft() 
        if not self.count[order]: 
            self.order_id.pop( order ) 
            self.list.remove(order)
        return (id , *order)
    

    def clear(self) : 
        self.list.clear() 
        self.count.clear()
        self.order_id.clear()


    def __iter__(self) : 
        return iter(self.list) 

    exact_match = remove_order
        
    def relaxed_match(self,order:tuple)->Optional[tuple]: 
        candidates = SortedList() 
        sp , q = order 
        idx1 = self.list.bisect_left((sp-(beta*sp)/100,0))
        idx2 = self.list.bisect_left((sp+(beta*sp)/100,int(1e4)))
        if (idx1 == len(self.list)) or (idx2 == 0) or ( idx2 <= idx1 ): 
            return None 
        
        for i in range(idx1,idx2) :
            candidates.add((abs(self.list[i][0] - sp) , abs(self.list[i][1] - q ) , i ) )
            
        _ , _ , ans_idx = candidates[0]  

        return self.remove_order(self.list[ans_idx]) 
    
    def __str__(self) -> str:
        return '{:10s}:{}\n{:10s}:{}\n{:10s}:{}'.format(
            'List' , self.list, 
            'Count' , self.count , 
            'Order-ID' , self.order_id 
        )



#keep order containers : 
class AllOrderContainers: 
    
    def __init__(self) -> None: 
        self.main = {
            'quote_cancellations' : {}  ,
            'def_quote_cancellations' : {} ,  
            'possible_quotes_cancellations' : {} ,  
            'possible_def_quote_def_Cancellations' : {}, 
            'new_orders' : {}  ,
            'new_def_orders' : {} 
        }
        self.book_ids = SortedList()
        self.used_order_id_count = Counter()
        self.cancled_order_id_updates = {}
    
        self.new_ticks_containers = [
            'new_orders',
            'new_def_orders'
        ]

        self.cancellations_containers = [
            'quote_cancellations',
            'def_quote_cancellations',
            'possible_quotes_cancellations',
            'possible_def_quote_def_Cancellations'
        ]
    
    def remove_order_id(self,order_id)->None:
        self.used_order_id_count[order_id] = 1 
        
        
    def clear(self)->None: 
        for key in self.main : 
            self.main[key].clear()
        self.used_order_id_count.clear()
        self.cancled_order_id_updates.clear()
        self.book_ids.clear()
        
    def add_id(self,ask_id:int)->None:
        for key in self.main : 
            self.main[key][ask_id] = OrderContainer()
        self.book_ids.add(ask_id) 
    
    def book_id_exist(self,id:int)->bool :
        return ( id in self.book_ids )
            
    def clear_id(self,ask_id:int)->None: 
        if self.book_id_exist( ask_id ) : 
            for key in self.main :
                del self.main[key][ask_id]
            self.book_ids.remove(ask_id)
        else : 
            raise ValueError(f"ask id {ask_id} is not present in all containers.")
            
    def specific_order_incertion(
        self, 
        order:tuple, 
        num_updates:int,
        order_id,
        id:int, 
        type:str = 'quote_cancellations' 
    )->None : 
        '''
            Function that loggs a cancellation order to a cancellation container. 
        '''

        if type not in self.main.keys():
            raise ValueError(f'Invalid type {type} for cancellation order')
        
        # if start_id not in self.main[type] : 
        #     raise ValueError(f'Container quote_cancellations does not exist for ask_id {start_id}')
        # if end_id not in self.main[type] : 
        #     raise ValueError(f'Container def_quote_cancellations does not exist for ask_id {end_id}')
        
        if not self.book_id_exist(id) : 
            return 
        
        self.main[type][id].insert(
            order = order,
            order_id = order_id 
        )
            
        if type in self.cancellations_containers : 
            self.cancled_order_id_updates[order_id] = num_updates
        
    def add_cancellation_order(
        self, 
        order_dict:dict, 
        order_id,
        start_id:int, 
        end_id:int,
        type:str = 'quote_cancellations'
    )->None:
        
        for id in self.book_ids : 
            
            if id < start_id : 
                continue 
            if id > end_id : 
                break 
            
            if type == 'quote_cancellations' :
                self.specific_order_incertion(
                    order = (order_dict['sp'], order_dict['qty']), 
                    num_updates= order_dict['num_updates'],
                    order_id = order_id,
                    id = id ,
                    type = type 
                )
            elif type == 'def_quote_cancellations' :
                self.specific_order_incertion(
                    order = (order_dict['sp'], order_dict['qty']), 
                    num_updates= order_dict['num_updates'],
                    order_id = order_id,
                    id = id ,
                    type = type 
                )
            elif type == 'possible_cancellations' :
                if order_dict['sp1'] is not None :
                    self.specific_order_incertion(
                        order = (order_dict['sp1'], order_dict['qty']), 
                        num_updates= 0,
                        order_id = order_id,
                        id = id ,
                        type = 'possible_quotes_cancellations'
                    )
                if order_dict['sp2'] is not None :
                    self.specific_order_incertion(
                        order = (order_dict['sp2'], order_dict['qty']), 
                        num_updates= 0,
                        order_id = order_id,
                        id = id ,
                        type = 'possible_def_quote_def_Cancellations'
                    )
            else : 
                raise ValueError(f'Invalid type {type} for cancellation order')
             
        
    def add_new_order(
        self, 
        near_ask:dict,
        row:pd.Series, 
        possible_quotes:dict
    )->None : 
        
        
        id = self.book_ids[-1] # latest updated near book id ; # part of our assumpion ; 
        if ( possible_dict := make_possible_quote_dict(row,near_ask, id)) is not None : 
            possible_quotes[id][row['oid1']] = possible_dict 
        
        for id in self.book_ids : 
            if( possible_dict := make_possible_quote_dict(row, near_ask, id) ) is None :
                continue
            
            # add the new orders to all the currenty active near ask ids :
            # for normal quotes :
            self.specific_order_incertion(
                order = (possible_dict['sp1'], row['qty']),
                num_updates = -1 , 
                order_id = row['oid1'],
                id = id , 
                type = 'new_orders'
            )
            
            # for defensive quotes : 
            if possible_dict['sp2'] is not None :
                self.specific_order_incertion(
                    order = (possible_dict['sp2'], row['qty']),
                    num_updates = -1 , 
                    order_id = row['oid1'],
                    id = id ,
                    type = 'new_def_orders'
                )
        
    def get_match(
        self,
        container_name:str,
        ask_id:int,
        order:tuple, 
        match_type:str = 'exact_match'
        )->Optional[tuple]:
        
        if container_name not in self.main :
            raise ValueError(f'Container {container_name} does not exist')
        
        if ask_id not in self.book_ids : 
            return None 
        if match_type not in ['exact_match', 'relaxed_match'] : 
            raise ValueError(f'Invalid match type {match_type} for container {container_name}')
        
        match_function = getattr(self.main[container_name][ask_id], match_type)
        
        while True : 
            match = match_function(order)
            if match is None : 
                return None 
            if ( not self.used_order_id_count[match[0]] ): 
                self.used_order_id_count[match[0]] = 1 
                break
        ''' 
        Return order_id and the number of sucessful verifed 
        updates if the matcheds are done for cancellation orders. 
        
        If matched are made from the new orders containers,
        then return the order_id, the spread and the quantity. 
        '''
        order_id = match[0]
        if container_name in self.cancellations_containers : 
            return order_id , self.cancled_order_id_updates.pop(order_id)
        else : 
            return match 
            
    
    ## New ticks can belong to any ask id that is open at the time : 
    ## Cancelled ticks can belong to any ask id from their original ask id + 1 to the 
    # next most nearest verified ask id 
        
        
    def __iter__(self):
        return iter(self.main['quote_cancellations'].keys())
    
    def __getitem__(self, idx:int) : 
        return list(self.main['quote_cancellations'])[idx]
    
all_conntainers = AllOrderContainers()


def modify_tick_with_no_active_windows(
    row : pd.Series, 
    near_ask:dict, 
    near_ask_id:int, 
    quote:dict, 
    def_quote:dict, 
    total_quoting_orders:dict, 
    possible_quotes:dict , 
    counts : list , f , time , order_id_map:dict
) : 
    
    # get the order id from the row :
    if row['oid1'] not in order_id_map :
        # order id is not present in any of the containers :
        # logg only if the order qty lies within whatever is available in the near_ask_book
        order_id = row['oid1']
        if ((foo := make_possible_quote_dict(row, near_ask, near_ask_id)) is not None) and (row['level'] <= max_level) : 
            possible_quotes[near_ask_id][order_id] = foo
            order_id_map[order_id] = order_id
            f.write(f"{time},ModifyTickWithNoWindow,{row['oid1']},AddtoPossibleOrders,{counts[0]},{counts[1]},{near_ask_id}\n")
        return  counts
    
    order_id = order_id_map[row['oid1']]
    
    possible_dict = make_possible_quote_dict(row,near_ask,near_ask_id)
    
    # if order is a known quoting order : 
    if order_id in quote[near_ask_id] : 
        # has more than the required threshold values 
        # update total orders : 
        total_quoting_orders[quote[near_ask_id][order_id]['sp'] ] -= quote[near_ask_id][order_id]['qty']
        counts[0] -= quote[near_ask_id][order_id]['qty']
        if total_quoting_orders[quote[near_ask_id][order_id]['sp'] ] < 1 : 
            del total_quoting_orders[quote[near_ask_id][order_id]['sp']] 
        
        if quote[near_ask_id][order_id]['num_updates'] >= theta : 
            # keep the modificatied order as a quoting order : 
            foo = make_quote_dict(
                possible_dict, 
                num_updates=quote[near_ask_id][order_id]['num_updates']
            )
            # keep the new order only if the quantity lies withing the 
            # top of the near book limit ( 5 levels )
            if foo : 
                total_quoting_orders[foo['sp']] =\
                total_quoting_orders.get( foo['sp'] , 0 ) + foo['qty']
                counts[0] += foo['qty']
                counts[1] += foo['qty'] - quote[near_ask_id][order_id]['qty']
                quote[near_ask_id][order_id] = foo 
                f.write(f"{time},ModifyTickWithNoWindow,{row['oid1']},KeepQuoteOrder,{counts[0]},{counts[1]},{near_ask_id}\n")
            else : 
                counts[1] -= quote[near_ask_id][order_id]['qty']
                f.write(f"{time},ModifyTickWithNoWindow,{row['oid1']},RemoveQuoteOrder,{counts[0]},{counts[1]},{near_ask_id}\n")
                del order_id_map[row['oid1']]
                del quote[near_ask_id][order_id] 
        else : 
            # treat is as a new tick itself : 
            counts[1] -= quote[near_ask_id][order_id]['qty']
            if possible_dict :
                possible_quotes[near_ask_id][order_id] = possible_dict 
                f.write(f"{time},ModifyTickWithNoWindow,{row['oid1']},MoveQuotetoPossible,{counts[0]},{counts[1]},{near_ask_id}\n")
            else :  
                f.write(f"{time},ModifyTickWithNoWindow,{row['oid1']},RemoveQuoteOrder,{counts[0]},{counts[1]},{near_ask_id}\n")
                del order_id_map[row['oid1']]
            del quote[near_ask_id][order_id]
    # if order is a known defencive quoting order : 
    elif order_id in def_quote[near_ask_id] : 
        # update total orders : 
        total_quoting_orders[def_quote[near_ask_id][order_id]['sp'] ] -=\
        def_quote[near_ask_id][order_id]['qty']
        counts[0] -= def_quote[near_ask_id][order_id]['qty']
        if total_quoting_orders[def_quote[near_ask_id][order_id]['sp'] ] < 1 : 
            total_quoting_orders.pop(def_quote[near_ask_id][order_id]['sp'])
            
        if def_quote[near_ask_id][order_id]['num_updates'] >= theta : 
        # keep the modification in quote 
            foo = make_def_quote_dict(
                possible_dict, 
                num_updates= def_quote[near_ask_id][order_id]['num_updates']
            )
            if foo : 
                total_quoting_orders[foo['sp']] =\
                total_quoting_orders.get( foo['sp'] , 0 ) + foo['qty']
                counts[0] += foo['qty']
                counts[1] += foo['qty'] - def_quote[near_ask_id][order_id]['qty']
                def_quote[near_ask_id][order_id] = foo 
                f.write(f"{time},ModifyTickWithNoWindow,{row['oid1']},KeepDefQuoteOrder,{counts[0]},{counts[1]},{near_ask_id}\n")
            else : 
                counts[1] -= def_quote[near_ask_id][order_id]['qty'] 
                del def_quote[near_ask_id][order_id] 
                f.write(f"{time},ModifyTickWithNoWindow,{row['oid1']},RemoveDefQuoteOrder,{counts[0]},{counts[1]},{near_ask_id}\n")
                del order_id_map[row['oid1']]
        else : 
            counts[1] -= def_quote[near_ask_id][order_id]['qty'] 
            if possible_dict : 
            # again, treat this as a new order itself : 
                possible_quotes[near_ask_id][order_id] = possible_dict 
                f.write(f"{time},ModifyTickWithNoWindow,{row['oid1']},MoveDefQuotetoPossible,{counts[0]},{counts[1]},{near_ask_id}\n")
            else : 
                f.write(f"{time},ModifyTickWithNoWindow,{row['oid1']},RemoveDefQuoteOrder,{counts[0]},{counts[1]},{near_ask_id}\n")
                del order_id_map[row['oid1']]
            del def_quote[near_ask_id][order_id]
    # if the orders is logged into the possible quoting buffer : 
    elif order_id in possible_quotes[near_ask_id]: 
        # logg only if the order qty lies within whatever is available in the near_ask_book
        if (row['level'] <= max_level) and ((foo := make_possible_quote_dict(row, near_ask, near_ask_id)) is not None) :
            f.write(f"{time},ModifyTickWithNoWindow,{row['oid1']},KeepPossibleOrder,{counts[0]},{counts[1]},{near_ask_id}\n")
            possible_quotes[near_ask_id][order_id] = foo
        else : 
            # else drop it 
            f.write(f"{time},ModifyTickWithNoWindow,{row['oid1']},RemovePossibleOrder,{counts[0]},{counts[1]},{near_ask_id}\n")
            del order_id_map[row['oid1']]
            del possible_quotes[near_ask_id][order_id]
    # ir the order is not logges any where but is within the max level limit after modification : 
    elif (row['level'] <= max_level) and ((foo := make_possible_quote_dict(row, near_ask, near_ask_id)) is not None) : 
        # logg only if the order qty lies within whatever is available in the near_ask_book
        f.write(f"{time},ModifyTickWithNoWindow,{row['oid1']},AddtoPossibleOrders,{counts[0]},{counts[1]},{near_ask_id}\n")
        order_id_map[row['oid1']] = row['oid1']
        possible_quotes[near_ask_id][order_id] = foo
            
    return counts 


def modify_tick_with_trade(
    row : pd.Series, 
    near_ask:dict, 
    quote:dict, 
    def_quote:dict, 
    total_quoting_orders:dict, 
    possible_quotes:dict, 
    counts : list , f , time , order_id_map:dict , 
    all_conntainers : AllOrderContainers
) : 
    
    if row['oid1'] not in order_id_map :
        return counts 
    
    order_id = order_id_map[row['oid1']]
    for id in quote : 
        
        possible_dict = make_possible_quote_dict(row,near_ask,id)
        
        # if order is a known quoting order : 
        if order_id in quote[id] : 
            # has more than the required threshold values 
            # update total orders : 
            total_quoting_orders[quote[id][order_id]['sp'] ] -=\
            quote[id][order_id]['qty']
            counts[0] -= quote[id][order_id]['qty']
            if total_quoting_orders[quote[id][order_id]['sp'] ] < 1 : 
                del total_quoting_orders[quote[id][order_id]['sp']] 
                
            # if quote[id][order_id]['num_updates'] >= theta : 
                # keep the modificatied order as a quoting order : 
            foo = make_quote_dict(
                possible_dict, 
                num_updates=quote[id][order_id]['num_updates']
            )
            # keep the new order only if the quantity lies withing the 
            # top of the near book limit ( 5 levels )
            if foo : 
                total_quoting_orders[foo['sp']] = total_quoting_orders.get( foo['sp'] , 0 ) + foo['qty']
                counts[0] += foo['qty']
                counts[1] += foo['qty'] - quote[id][order_id]['qty']
                quote[id][order_id] = foo 
                f.write(f"{time},TradeWindow_Modify_Order,{row['oid1']},KeepQuoteOrder,{counts[0]},{counts[1]},{id}\n")
            else : 
                counts[1] -= quote[id][order_id]['qty']
                f.write(f"{time},TradeWindow_Modify_Order,{row['oid1']},RemoveQuoteOrder,{counts[0]},{counts[1]},{id}\n")
                del order_id_map[row['oid1']]
                del quote[id][order_id] 
            # else : 
            #     # treat is as a new tick itself : 
            #     counts[1] -= quote[id][order_id]['qty']
            #     if possible_dict :
            #         possible_quotes[id][order_id] = possible_dict 
            #         f.write(f"{time},TradeWindow_Modify_Order,{row['oid1']},MoveQuotetoPossible,{counts[0]},{counts[1]},{id}\n")
            #     else : 
            #         f.write(f"{time},TradeWindow_Modify_Order,{row['oid1']},RemoveQuoteOrder,{counts[0]},{counts[1]},{id}\n")
            #     del quote[id][order_id]
            break 
        # if order is a known defencive quoting order : 
        elif order_id in def_quote[id] : 
            # update total orders : 
            total_quoting_orders[def_quote[id][order_id]['sp'] ] -=\
            def_quote[id][order_id]['qty']
            counts[0] -= def_quote[id][order_id]['qty']
            if total_quoting_orders[def_quote[id][order_id]['sp'] ] < 1 : 
                total_quoting_orders.pop(def_quote[id][order_id]['sp'])
                
            # if def_quote[id][order_id]['num_updates'] >= theta : 
            # keep the modification in quote 
            foo = make_def_quote_dict(
                possible_dict, 
                num_updates= quote[id][order_id]['num_updates']
            )
            if foo : 
                total_quoting_orders[foo['sp']] =\
                total_quoting_orders.get( foo['sp'] , 0 ) + foo['qty']
                counts[0] += foo['qty']
                counts[1] += foo['qty'] - def_quote[id][order_id]['qty']
                def_quote[id][order_id] = foo 
                f.write(f"{time},TradeWindow_Modify_Order,{row['oid1']},KeepDefQuoteOrder,{counts[0]},{counts[1]},{id}\n")
            else : 
                counts[1] -= def_quote[id][order_id]['qty']
                f.write(f"{time},TradeWindow_Modify_Order,{row['oid1']},RemoveDefQuoteOrder,{counts[0]},{counts[1]},{id}\n")
                del def_quote[id][order_id] 
                del order_id_map[row['oid1']]
            # else : 
            #     counts[1] -= def_quote[id][order_id]['qty']
            #     if possible_dict : 
            #     # again, treat this as a new order itself : 
            #         possible_quotes[id][order_id] = possible_dict 
            #         f.write(f"{time},TradeWindow_Modify_Order,{row['oid1']},MoveDefQuotetoPossible,{counts[0]},{counts[1]},{id}\n")
            #     else : 
            #         f.write(f"{time},TradeWindow_Modify_Order,{row['oid1']},RemoveDefQuoteOrder,{counts[0]},{counts[1]},{id}\n")
            #     del def_quote[id][order_id]
            break 
        # if the orders is logged into the possible quoting buffer : 
        elif order_id in possible_quotes[id]: 
            # logg only if the order qty lies within whatever is available in the near_ask_book
            if (row['level'] <= max_level) and ((foo := make_possible_quote_dict(row, near_ask, id)) is not None) :
                all_conntainers.remove_order_id(order_id)
                new_order_id = next(script_order_id_counter)
                order_id_map[row['oid1']] = new_order_id
                possible_quotes[id][new_order_id] = foo
                del possible_quotes[id][order_id]
                f.write(f"{time},TradeWindow_Modify_Order,{new_order_id},KeepPossibleOrder,{counts[0]},{counts[1]},{id}\n")
            else : 
                # else drop it 
                del order_id_map[row['oid1']]
                del possible_quotes[id][order_id]
                f.write(f"{time},TradeWindow_Modify_Order,{row['oid1']},RemovePossibleOrder,{counts[0]},{counts[1]},{id}\n")
            break 
        
    return counts 


prev_time_check = 0 
time_check = 0 

TotalTime  = 22500000000000


from IPython.display import clear_output
clear_output(wait=True)


new_tick_counts = 0 
# cumilative_total_quotes = 0 

new_ticks_list = []
# total_qts = []

foo1 , foo2 = 0 , 0 
num_verified = 0 


def reduce_total_quoting_orders(
    total_quoting_orders:dict, 
    sp, qty
)->None:
    if sp in total_quoting_orders : 
        total_quoting_orders[sp] -= qty 
        if total_quoting_orders[sp] < 1 : 
            del total_quoting_orders[sp]
    else : 
        raise ValueError(f'Spread {sp} not found in total quoting orders ... ')
    
def remove_order_id_from_possible_quotes(
    possible_quotes:dict, 
    order_id
)->None:
    for val in possible_quotes.values() : 
        if order_id in val : 
            del val[order_id]
            return
        
max_far_qty_top = -1 

max_foo1 = -1 

current_percentage = 0
top_of_far_ask = -1


pbar = tqdm(
    total=100, 
    ncols=150, 
    bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}'
    )


result_file = '_'.join([inputs['underlying'], inputs['date'], 'result.csv'])
result_file = os.path.join( inputs['result_path'] , result_file )

log_file = '_'.join([inputs['underlying'], inputs['date'], 'log.csv'])
log_file = os.path.join( inputs['log_path'] , log_file ) 


with open( log_file , 'w' ) as f , open( result_file , 'w') as f_result : 
    
    f.write('Time,EventType,Oid,ActionTaken,foo1,foo2,UpdateId\n')
    f_result.write('Time,SpreadPrice,Qty,CurrentBookSpread,TotalQuoteOrders,FarAskDepth,TickSize\n')
    
    for chunk in get_iter(): 

        chunk = chunk[(chunk['contract_name'] == near_symbol) | (chunk['contract_name'] == far_symbol ) ].reset_index(drop=True)
        chunk.insert(0,'Time',chunk['exchange_epoch_nanos'].apply(convert_ns_to_time))
        # InMarketTime -> time in mili seconds after market starts !! 
        chunk.insert(0,'InMarketTime' , (((chunk['exchange_epoch_nanos']%(int(24*36*1e11)) - int(13500*1e9)) )).astype(int) )
        chunk[qty_cols] /= lotsize 
        chunk[book_cols] = chunk[book_cols].fillna(0).astype( float )
        chunk[['oid1' , 'oid2']] = chunk[['oid1' , 'oid2']].fillna(-1).astype(int) 

        i = 0
        n = chunk.shape[0]
    
            
        while i < n : 
            
            max_foo1 = max( max_foo1 , foo1 ) 
            row = chunk.iloc[ i , : ]
            time = row['InMarketTime'] 
            
            
            # update bar : 
            if i % 1000 == 0 : 
                time = min( TotalTime , time )
                percentage_done = round((time/TotalTime)*100,4)
                delta = percentage_done - current_percentage
                if delta > 0 : 
                    pbar.update(delta)
                    current_percentage = percentage_done
                pbar.set_postfix_str("Time: " + row['Time'])
            
            if time > TotalTime : 
                break 
            
    
            time_check = row['InMarketTime']//T
            # print( row['InMarketTime'] )
            # print( quote ) 
            # print( all_conntainers.book_ids )
            # print( total_quoting_orders )
            # print('-'*30)
            
            if row['contract_name'] == far_symbol : 
                top_of_far_ask = row['ap1']
            
            trade_flag = False 
            
            # logic to handel trades : 
            if( row['event_type'] == 'TRADE_SUMMARY')  and (row['contract_name'] == far_symbol ) : 
                time = row['InMarketTime'] 
                i += 2 
                row = chunk.iloc[ i , : ]
                while (i < n - 1 ) and (time == row['InMarketTime']):
                    trade_flag = True 
                    # treat this like there is no running update window : 
                    # For NEW TICK :  just log them into the possible quoting order candidates : 
                    # if (row['event_type'] == 'NEW_TICK') and (row['level'] <= max_level ) :  
                    #     if (processed_dict := make_possible_quote_dict(row,near_ask,near_ask_id)) : 
                    #         # consider only if the quantiy is acceptable. 
                    #         possible_quotes[near_ask_id][row['oid1']]  = processed_dict 
                    # FOR TICK CANCELATION  : 
                    
                    
                    if (row['event_type'] == 'CANCEL_TICK') : 
                        
                        if row['oid1'] not in order_id_map :
                        # order id is not present in any of the containers :
                            i += 1 
                            row = chunk.iloc[ i , : ]
                            continue
                        
                        # get the order id from the map :
                        order_id = order_id_map[row['oid1']] 
                        del order_id_map[row['oid1']]
                        
                        flag = True
                        
                        for id in quote : 
                            if order_id in quote[id] : 
                                foo1 -= quote[id][order_id]['qty']
                                reduce_total_quoting_orders(
                                    total_quoting_orders=total_quoting_orders,
                                    sp=quote[id][order_id]['sp'],
                                    qty=quote[id][order_id]['qty']
                                )
                                foo2 -= quote[id][order_id]['qty']
                                del quote[id][order_id] 
                                f.write(f"{time},TradeWindow_Cancel_Order,{row['oid1']},RemoveQuoteOrder,{foo1},{foo2},{id}\n")
                                flag = False
                                break 
                            elif order_id in def_quote[id] : 
                                qty = def_quote[id][order_id]['qty']
                                foo1 -= qty
                                reduce_total_quoting_orders(
                                    total_quoting_orders=total_quoting_orders,
                                    sp=def_quote[id][order_id]['sp'],
                                    qty=qty
                                )
                                foo2 -= qty 
                                del def_quote[id][order_id]
                                f.write(f"{time},TradeWindow_Cancel_Order,{row['oid1']},RemoveDefQuoteOrder,{foo1},{foo2},{id}\n")
                                flag = False
                                break 
                            elif order_id in possible_quotes[id] : 
                                f.write(f"{time},TradeWindow_Cancel_Order,{row['oid1']},RemovePossibleOrder,{foo1},{foo2},{id}\n")
                                all_conntainers.remove_order_id(order_id)
                                del possible_quotes[id][order_id]
                                flag = False
                                break 
                            
                        if flag :
                            all_conntainers.remove_order_id(order_id)
                            
                    # FOR TICK MODFICATION : 
                    elif (row['event_type'] == 'MODIFY_TICK'): 
                        foo1 , foo2 = modify_tick_with_trade(
                            row = row , 
                            near_ask=near_ask, 
                            quote=quote, 
                            def_quote=def_quote,
                            total_quoting_orders=total_quoting_orders,
                            possible_quotes=possible_quotes ,
                            counts = [ foo1 , foo2 ] , f = f ,
                            time = time , order_id_map=order_id_map,
                            all_conntainers=all_conntainers
                        )
                        
                    i += 1 
                    row = chunk.iloc[ i , : ]
                
            if trade_flag : 
                trade_flag = False 
                continue
            
            # check if a candidate update is verified :
            if (possible_update_candidate_id != 0 ) and (row['InMarketTime'] >= time_to_verification) : 
                # if( not len(verified_updates_queue) ) : 
                #     first_near_ask_id_window = ask_id 
                # promote to verified_updates_queue :
                verified_buffer.insert(
                    id = possible_update_candidate_id , 
                    time = time_to_verification - t1 + t2 
                )
                # print('-'*20)
                # print( verified_buffer )
                # print( 'Verified:::' , possible_update_candidate_id )
                # print('-'*20)
                f.write(f"{time},VerifiedNearOrderBook,{row['oid1']},--,{foo1},{foo2},{possible_update_candidate_id}\n")
                possible_update_candidate_id = 0 
                time_to_verification = -1 
                
                num_verified += 1   
                
            while (verified_buffer.not_empty()) and (row['InMarketTime'] >= verified_buffer.latest_exp()) :
                terminated_id = verified_buffer.pop()
                
                f.write(f"{time},VerifiedUpdateEnd,--,--,{foo1},{foo2},{terminated_id}\n")
                
                # clear the terminated id from all containers : 
                all_conntainers.clear_id( terminated_id )
                last_verified_id = verified_buffer.last_verified_id(terminated_id)
                # for all the ids from last_verified_id to verified_id - 1 simpley promote all the known 
                # quoting orders, known defencive quoting orders and possible quoting orders (that are not updated) 
                # to the latest finished verified update id, in case the orders does not required any update : 
                book_at_terminated_id = near_ask[terminated_id]
                for id in range(last_verified_id,terminated_id) : 
                    book_at_id = near_ask[id]
                    
                    # first row where the index differs for normal quoting orders 
                    diff = np.any( book_at_id['book'] != book_at_terminated_id['book'] , axis = 1 ) 
                    first_row = np.argmax( diff ) if np.any( diff ) else None 
                    if first_row is None :
                        # no change in the book!  
                        max_qty_to_keep = book_at_id['total_qty']
                    else : 
                        max_qty_to_keep = book_at_id['qty_cumsum'][first_row-1] if first_row > 0 else 0 
                        if ( book_at_id['book'][first_row,0] == book_at_terminated_id['book'][first_row,0]) : 
                            max_qty_to_keep += min(book_at_id['book'][first_row,1] , book_at_terminated_id['book'][first_row,1])
                    
                    # first row where the index differs for def quoting orders  
                    diff = np.any( book_at_id['book'][1: , :] != book_at_terminated_id['book'][1:, :] , axis = 1 ) 
                    first_row = np.argmax( diff )+1 if np.any( diff ) else None 
                    if first_row is None : 
                        max_qty_to_keep_def = book_at_id['total_def_qty']
                    else : 
                        max_qty_to_keep_def = book_at_id['qty_cumsum'][first_row-1] - book_at_id['qty_cumsum'][0] if first_row > 1 else 0 
                        if( book_at_id['book'][first_row,0] == book_at_terminated_id['book'][first_row,0]) : 
                            max_qty_to_keep_def += min(book_at_id['book'][first_row,1] , book_at_terminated_id['book'][first_row,1])
                        
                    
                    for dict_type , max_qty in zip([quote,def_quote] , [max_qty_to_keep,max_qty_to_keep_def]) : 
                        for order_id , foo in dict_type[id].items() : 
                            if foo['qty'] <= max_qty : 
                                # promote to now latest terminated dict : 
                                f.write(f"{time},VerifiedUpdateEnd,{order_id},PromoteOrderWithNoChange,{foo1},{foo2},{id}\n")
                                dict_type[terminated_id][order_id] = foo 
                            else : 
                                # remove the order 
                                # print( total_quoting_orders )
                                reduce_total_quoting_orders(
                                    total_quoting_orders=total_quoting_orders,
                                    sp=foo['sp'],
                                    qty=foo['qty']
                                )
                                foo1 -= foo['qty']
                                foo2 -= foo['qty']
                                f.write(f"{time},VerifiedUpdateEnd,{order_id},RemoveNotUpdatedOrder,{foo1},{foo2},{id}\n")
                                
                    for order_id , foo in possible_quotes[id].items() : 
                        if foo['qty'] > max( max_qty_to_keep_def , max_qty_to_keep ) : 
                            # simply drop this 
                            f.write(f"{time},VerifiedUpdateEnd,{order_id},RemovePossibleOrder,{foo1},{foo2},{id}\n")
                            continue
                        elif foo['qty'] <= min( max_qty_to_keep , max_qty_to_keep_def ) : 
                            # promote to the latest verified terminated update id 
                            pass
                        elif foo['qty'] <= max_qty_to_keep : 
                            # promote to the latest verified terminated update id 
                            foo['sp2'] = None 
                        else : 
                            # promote to the latest verified terminated update id 
                            if foo['sp2'] == None : 
                                f.write(f"{time},VerifiedUpdateEnd,{order_id},RemovePossibleOrder,{foo1},{foo2},{id}\n")
                                continue
                            else : 
                                # cannot be a normal quoting order but may be a 
                                # possible defencive quoting order 
                                foo['sp1'] = None 
                                
                        # promote to the latest verified terminated update id
                        f.write(f"{time},VerifiedUpdateEnd,{order_id},PromotePossibleOrder,{foo1},{foo2},{id}\n")
                        possible_quotes[terminated_id][order_id] = foo 
                        
                    del quote[id]
                    del def_quote[id]
                    del possible_quotes[id] 
                    
                    
                
                if ( not verified_buffer.not_empty() ) and ( possible_update_candidate_id == 0 ) : 
                    # end of a global update          
                    verified_buffer.clear() 
                    # all_conntainers.clear() 
                    # print('**end of window**' , disqualified_buffer.not_empty() ) 
                
            while (disqualified_buffer.not_empty()) and (row['InMarketTime'] >= disqualified_buffer.latest_exp()) :
                f.write(f"{time},UpdateDisqualified,--,--,{foo1},{foo2},{disqualified_buffer[0]}\n")
                all_conntainers.clear_id( disqualified_buffer.pop() ) 
                
            # if no update windows are running save the total quoting orders to a pickle file : 
            if (not verified_buffer.not_empty()) and ( possible_update_candidate_id == 0 ) and (time_check != prev_time_check) : 
            # if (time_check < prev_time_check ) : 
                prev_time_check = time_check
                # pickle_file_name = get_pickel_file_name(inputs,time_check)
                temp1 = 0 
                temp2 = 0 
                for val in total_quoting_orders.values() : 
                    temp1 += val 
                for id in quote.values() : 
                    for order in id.values() : 
                        temp2 += order['qty']
                for id in def_quote.values() : 
                    for order in id.values() : 
                        temp2 += order['qty']  
                
                if temp1 == 0 : 
                    f_result.write(f"{row['Time']},--,--,{round(top_of_far_ask-near_ask[near_ask_id]['book'][0,0],2)},{0},{near_ask[near_ask_id]['total_qty']},{ticksize}\n")
                else : 
                    for sp , qty in total_quoting_orders.items() :
                        f_result.write(f"{row['Time']},{sp},{qty},{round(top_of_far_ask-near_ask[near_ask_id]['book'][0,0],2)},{temp1},{near_ask[near_ask_id]['total_qty']},{ticksize}\n")
                    
                # with open( pickle_file_name , 'wb' ) as foo : 
                #     pickle.dump( 
                #     {'data' : total_quoting_orders, 
                #     'time' : row['Time'] , 
                #     'temp1' : temp1 , 
                #     'temp2' : temp2 , 
                #     'num_verified' : num_verified , 
                #     'total_qty' : near_ask[near_ask_id]['total_qty']}, foo )
                max_far_qty_top = max( max_far_qty_top , near_ask[near_ask_id]['total_qty'] )
                new_ticks_list.append( new_tick_counts )
                new_tick_counts = 0 
            
            if row['event_type'] == 'NEW_TICK' : 
                new_tick_counts += 1 
                
            # check if the near ask side is updated :
            if (row['contract_name'] == near_symbol) and (row['side'] == 'SELL'): 

                new_ask_book = row[ask_cols].to_numpy().reshape((5,3)) 
                if (new_ask_book != near_ask[near_ask_id] ).any() : 
                    # if (not verified_buffer.not_empty()) and (not possible_update_candidate_id) : 
                        # print('**start of window**')
                    near_ask_id += 1  
                    # add to near_ask_id: 
                    near_ask[near_ask_id] = make_book_dict(new_ask_book)

                    f.write(f"{time},NewNearBookUpdate,--,--,{foo1},{foo2},{near_ask_id}\n")
                    
                    # see if this update initiates a new 'window' : 
                    # if (not verified_buffer.not_empty()) and (not possible_update_candidate_id ) : 
                    #     first_near_ask_id_window = near_ask_id 
                    
                    quote[near_ask_id] = {} 
                    def_quote[near_ask_id] = {} 
                    possible_quotes[near_ask_id] = {} 
                    
                    all_conntainers.add_id( near_ask_id )
                    
                    # add this update to possible_update_queue : // later promoted to verified 
                    #   updates if it stays for more than time t1 
                    if possible_update_candidate_id : 
                        # if there are more than one updates in the queue , then we need to disqualify the previous ones : 
                        f.write(f"{time},NewNearBookUpdate,--,UpdateDisqualified,{foo1},{foo2},{possible_update_candidate_id}\n")
                        disqualified_buffer.insert(
                            id = possible_update_candidate_id , 
                            time = row['InMarketTime'] + mu 
                        )
                        # print( 'Disqualified:::' , possible_update_candidate_id )
                        
                    possible_update_candidate_id = near_ask_id 
                    time_to_verification = row['InMarketTime'] + t1 
                    
            # check the far ask side for possible bidding orders : 
            elif (row['contract_name'] == far_symbol) and (row['side'] == 'SELL') :

                # no running update window : 
                if (not verified_buffer.not_empty()) and (not possible_update_candidate_id): 
                    # For NEW TICK :  just log them into the possible quoting order candidates : 
                    if (row['event_type'] == 'NEW_TICK') and (row['level'] <= max_level ) :  
                        if (processed_dict := make_possible_quote_dict(row,near_ask,near_ask_id)) : 
                            # consider only if the quantiy is acceptable. 
                            order_id = row['oid1']
                            order_id_map[order_id] = order_id
                            f.write(f"{time},NewTickWithNoWindow,{order_id},AddtoPossibleQuotes,{foo1},{foo2},{near_ask_id}\n")
                            possible_quotes[near_ask_id][order_id]  = processed_dict 
                    # FOR TICK CANCELATION  : 
                    elif (row['event_type'] == 'CANCEL_TICK') : 
                        
                        # get the order id from the map :
                        if row['oid1'] not in order_id_map :
                            # order id is not present in any of the containers :
                            i += 1 
                            continue
                        
                        order_id = order_id_map[row['oid1']] 
                        del order_id_map[row['oid1']]
                        
                        
                        if order_id in quote[near_ask_id] : 
                            qty = quote[near_ask_id][order_id]['qty']
                            foo1 -= qty
                            foo2 -= qty
                            reduce_total_quoting_orders(
                                total_quoting_orders=total_quoting_orders,
                                sp=quote[near_ask_id][order_id]['sp'],
                                qty=qty
                            )
                            del quote[near_ask_id][order_id]
                            f.write(f"{time},CancelTickWithNoWindow,{order_id},RemoveQuoteOrder,{foo1},{foo2},{near_ask_id}\n")
                            
                        elif order_id in def_quote[near_ask_id] : 
                            qty = def_quote[near_ask_id][order_id]['qty']
                            foo1 -= qty
                            foo2 -= qty
                            reduce_total_quoting_orders(
                                total_quoting_orders=total_quoting_orders,
                                sp=def_quote[near_ask_id][order_id]['sp'],
                                qty=qty
                            )
                            del def_quote[near_ask_id][order_id]
                            f.write(f"{time},CancelTickWithNoWindow,{order_id},RemoveDefQuoteOrder,{foo1},{foo2},{near_ask_id}\n")
                        
                        elif order_id in possible_quotes[near_ask_id] :
                            f.write(f"{time},CancelTickWithNoWindow,{order_id},RemovePossibleOrder,{foo1},{foo2},{near_ask_id}\n")
                            possible_quotes[near_ask_id].pop( order_id , None )
                    # FOR TICK MODFICATION : 
                    elif (row['event_type'] == 'MODIFY_TICK'): 
                        
                        foo1 , foo2 = modify_tick_with_no_active_windows(
                            row = row , 
                            near_ask=near_ask, 
                            quote=quote, 
                            def_quote=def_quote,
                            total_quoting_orders=total_quoting_orders,
                            possible_quotes=possible_quotes ,
                            counts = [ foo1 , foo2 ] , f = f ,
                            time = time , 
                            near_ask_id = near_ask_id,
                            order_id_map = order_id_map
                        )

                # running update windows are present : 
                else: 
                    # if the row is a new tick : 
                    if (row['event_type'] == 'NEW_TICK') : 
                        flag = False 
                        # first try to match it with previously logged cancellled orders in order of priority :
                        for active_id in all_conntainers :
                            if(possible_dict := make_possible_quote_dict(row,near_ask,active_id)) is None :
                                continue  
                            # first try for exact matches then for relaxed matches with the following prority : 
                            #   1. known quoting orders cancellations 
                            #   2. known defencive quoting orders cancellations
                            #   3. possible quoting orders cancellations
                            #   4. possible defencive quoting orders cancellations
                            for match_type in ['exact_match', 'relaxed_match'] :
                                for container_name in all_conntainers.cancellations_containers : 
                                    
                                    if container_name in ['quote_cancellations', 'possible_quotes_cancellations'] :
                                        spread_to_use = 'sp1'
                                        modification_function = make_quote_dict
                                        dictionary_to_use = quote
                                    else : 
                                        spread_to_use = 'sp2'
                                        modification_function = make_def_quote_dict
                                        dictionary_to_use = def_quote
                                        
                                    
                                    if possible_dict[spread_to_use] is None :
                                        continue
                                    
                                    if (match := all_conntainers.get_match(
                                        container_name=container_name,
                                        ask_id=active_id,
                                        order=(possible_dict[spread_to_use],possible_dict['qty']),
                                        match_type=match_type
                                    )) is not None : 
                                        # if a match is found, then update the respective dictionary with the new order : 
                                        _ , num_updates = match
                                        # update the total quoting orders :
                                        total_quoting_orders[possible_dict[spread_to_use]] =\
                                            total_quoting_orders.get(possible_dict[spread_to_use] , 0 ) + possible_dict['qty']
                                        foo1 += possible_dict['qty']
                                        foo2 += possible_dict['qty'] 
                                        # make map entry for the order id :
                                        order_id_map[row['oid1']] = row['oid1']
                                        # add this to quoting orders :
                                        dictionary_to_use[active_id][row['oid1']] = modification_function(possible_dict=possible_dict , num_updates = num_updates + 1)
                                        flag = True 
                                        f.write(f"{time},NewTickInWindow,{row['oid1']},MatchedQuoteOrder,{foo1},{foo2},{active_id}\n")
                                        break
                                if flag : 
                                    break
                            if flag :
                                break
                        if (not flag ) and (row['level'] <= max_level): 
                            # make map entry for the order id :
                            order_id_map[row['oid1']] = row['oid1']
                            # if no match is found, then logg it into the new orders container :
                            f.write(f"{time},NewTickInWindow,{row['oid1']},AddtoPossibleOrders&NewBuffer,{foo1},{foo2},{near_ask_id}\n")
                            all_conntainers.add_new_order(near_ask,row,possible_quotes) 
                    # if the row is a cancelation tick :
                    elif (row['event_type'] == 'CANCEL_TICK') :
                        
                        # find if the order is in either of the known quoting or 
                        # known defencive quoting orders or possible quoting orders :
                        if row['oid1'] not in order_id_map :
                            # this means that the order is not logged into any of the containers :
                            i += 1 
                            continue
                        
                        order_id = order_id_map[row['oid1']]
                        del order_id_map[row['oid1']]
                        
                        order_id_found = False
                        order_id_matched = False 
                        
                        for id in quote : 
                            
                            for dict_type , container_name in zip(
                                [quote , def_quote] , 
                                ['new_orders' , 'new_def_orders']
                            ): 
                                
                                if order_id not in dict_type[id] : 
                                    continue
                                # order belonds to the known quoting orders :
                                # update the total quoting orders :
                                
                                order_id_found = True
                                
                                reduce_total_quoting_orders(
                                    total_quoting_orders=total_quoting_orders,
                                    sp=dict_type[id][order_id]['sp'],
                                    qty=dict_type[id][order_id]['qty']
                                )
                                foo1 -= dict_type[id][order_id]['qty']
                                
                                # try to find a matching order in the new orders container :
                                start_id = id + 1 # just next id to be updated 
                                end_id = verified_buffer.next_verified_id(id) 
                                if end_id == -1 : 
                                    end_id = all_conntainers[-1]
                                
                                if (start_id > end_id ):
                                    # this is the the latest update to the book, no more recent update exist
                                    # this means that the orders is alredy upto date with 
                                    # the latest available modifications, we treat this like a 
                                    # cancellation order and remove it from the known quoting orders : 
                                    foo2 -= dict_type[id][order_id]['qty']
                                    f.write(f"{time},CancelTickInWindow,{order_id},RemoveQuoteOrder,{foo1},{foo2},{near_ask_id}\n")
                                    del dict_type[id][order_id]
                                    order_id_matched = True 
                                    break 
                                # try to find a match in the new orders container : 
                                # priotity for matching is detailed as follows : 
                                #  1. Overs update ids, first come first serve basis
                                #  2. Match type : excat match over relaxed match 
                                #  Note we match known quoting orders with the possible new orders container only and 
                                # known defencive quoting orders with the new defencive orders container only :
                                
                                for update_to_id in range(start_id, end_id+1) :
                                    if update_to_id not in all_conntainers.book_ids : 
                                        continue 
                                    for match_type in ['exact_match', 'relaxed_match'] :
                                        if ( match := all_conntainers.get_match(
                                            container_name=container_name,
                                            ask_id=update_to_id,
                                            order=(dict_type[id][order_id]['sp'],dict_type[id][order_id]['qty']),
                                            match_type=match_type
                                        ) ) is not None :
                                            
                                            order_id_matched = True
                                            
                                            # remove the matched order from the known possible quoting orders dict : 
                                            remove_order_id_from_possible_quotes(
                                                possible_quotes=possible_quotes,
                                                order_id=match[0]
                                            )
                                            
                                            # match is found, update the respective dictionary with the new order : 
                                            total_quoting_orders[match[1]] =\
                                                total_quoting_orders.get( match[1] , 0 ) +  match[2] 
                                            foo1 += match[2]
                                            # add this to quoting orders : 
                                            dict_type[update_to_id][match[0]] = {
                                                'sp'  : match[1] , 
                                                'qty' : match[2] , 
                                                'num_updates' : dict_type[id][order_id]['num_updates'] + 1
                                            }
                                            foo2 += match[2]
                                            # remove the canclled order from the known quoting orders :
                                            foo2 -= dict_type[id][order_id]['qty']
                                            f.write(f"{time},CancelTickInWindow,{match[0]},MatchedQuoteOrder,{foo1},{foo2},{update_to_id}\n")
                                            del dict_type[id][order_id]
                                            break
                                    if order_id_matched :
                                        break 
                                    
                                if order_id_found : 
                                    if not order_id_matched : 
                                        # then we log this order to the respective cancellation 
                                        # orders containers :
                                        all_conntainers.add_cancellation_order(
                                            order_dict = dict_type[id][order_id],
                                            order_id = order_id,
                                            start_id = start_id,
                                            end_id = end_id,
                                            type = 'quote_cancellations' if container_name == 'new_orders' else 'def_quote_cancellations'
                                        )
                                        foo2 -= dict_type[id][order_id]['qty']
                                        f.write(f"{time},CancelTickInWindow,{order_id},AddQuotetoCancelledBuffer,{foo1},{foo2},{id}\n")
                                        del dict_type[id][order_id]
                                    break 
                                
                            if order_id_found : 
                                break
                            
                            if order_id not in possible_quotes[id] : 
                                continue
                            # order belongs to the possible quoting orders : 
                            
                            order_id_found = True
                            
                            # remove the order from any containers that may have it :
                            all_conntainers.remove_order_id(order_id)
                            
                            # try to find a matching order in the new orders container :
                            start_id = id + 1 # just next id to be updated 
                            end_id = verified_buffer.next_verified_id(id) 
                            if end_id == -1 : 
                                end_id = all_conntainers[-1]
                                
                            if start_id > end_id :
                                # equivalent to id == near_ask_id : 
                                # this means that the orders is alredy upto date with 
                                # the latest available modifications, we treat this like a 
                                # cancellation order and remove it from the known quoting orders : 
                                f.write(f"{time},CancelTickInWindow,{order_id},RemovePossibleOrder,{foo1},{foo2},{id}\n")
                                del possible_quotes[id][order_id]
                                order_id_matched = True 
                                break 
                            
                            # try to find a match in the new orders container : 
                            # priotity for matching is detailed as follows : 
                            #  1. Overs update ids, first come first serve basis
                            #  2. Match type : excat match over relaxed match 
                            #  3. Match type : normal orders over defencive orders
                            #  Note we match known quoting orders with the possible new orders container only and 
                            # known defencive quoting orders with the new defencive orders container only :

                            for update_to_id in range(start_id, end_id+1) :
                                if not all_conntainers.book_id_exist(update_to_id) : 
                                    continue
                                for match_type in ['exact_match', 'relaxed_match'] :
                                    for container_name in all_conntainers.new_ticks_containers :
                                        
                                        if container_name == 'new_orders' :
                                            spread_to_use = 'sp1'
                                            dictionary_to_use = quote
                                        else : 
                                            spread_to_use = 'sp2'
                                            dictionary_to_use = def_quote
                                            
                                        if possible_quotes[id][order_id][spread_to_use] is None :
                                            continue
                                        
                                        if ( match := all_conntainers.get_match(
                                            container_name=container_name,
                                            ask_id=update_to_id,
                                            order=(possible_quotes[id][order_id][spread_to_use],possible_quotes[id][order_id]['qty']),
                                            match_type=match_type
                                        ) ) is not None :
                                            
                                            order_id_matched = True
                                            
                                            # remove the matched order from the known possible quoting orders dict : 
                                            remove_order_id_from_possible_quotes(
                                                possible_quotes=possible_quotes,
                                                order_id=match[0]
                                            )
                                            
                                            # match is found, update the respective dictionary with the new order : 
                                            total_quoting_orders[match[1]] =\
                                                total_quoting_orders.get( match[1] , 0 ) +  match[2] 
                                            foo1 += match[2]
                                            # add this to quoting orders : 
                                            dictionary_to_use[update_to_id][match[0]] = {
                                                'sp'  : match[1] , 
                                                'qty' : match[2] , 
                                                'num_updates' :  1
                                            }
                                            foo2 += match[2]
                                            if dictionary_to_use is quote :
                                                f.write(f"{time},CancelTickInWindow,{order_id},MovePossibletoQuote,{foo1},{foo2},{update_to_id}\n")
                                            else : 
                                                f.write(f"{time},CancelTickInWindow,{order_id},MovePossibletoDefQuote,{foo1},{foo2},{update_to_id}\n")
                                            # remove the canclled order from the known quoting orders :
                                            del possible_quotes[id][order_id]
                                            break
                                    if order_id_matched :
                                        break
                                if order_id_matched : 
                                    break 
                                
                            if order_id_found :
                                if not order_id_matched : 
                                    # then we log this order to the respective cancellation 
                                    # orders containers :
                                    all_conntainers.add_cancellation_order(
                                        order_dict = possible_quotes[id][order_id],
                                        order_id = next(script_order_id_counter),
                                        start_id = start_id,
                                        end_id = end_id,
                                        type = 'possible_cancellations'
                                    )
                                    f.write(f"{time},CancelTickInWindow,{order_id},AddPossibletoCancelledBuffer,{foo1},{foo2},{id}\n")
                                    del possible_quotes[id][order_id]
                                break                    
                    # if the row is a modify tick :
                    elif (row['event_type'] == 'MODIFY_TICK') :
                        
                        # find if the order is in either of the known quoting or 
                        # known defencive quoting orders or possible quoting orders :
                        
                        if row['oid1'] not in order_id_map :
                            # this means that the order is not logged into any of the containers before : 
                            i += 1 
                            continue
                        
                        order_id = order_id_map[row['oid1']]
                        
                        order_id_found = False 
                        order_id_matched = False 
                        
                        for id in quote : 
                            for dict_type in [quote , def_quote , possible_quotes ] : 
                                
                                if order_id not in dict_type[id] : 
                                    continue 
                                
                                # order is found 
                                order_id_found = True 
                                
                                
                                # if it is a quoting order the modification must be conjugate to an existing 
                                # near ask update window form id + 1 to the next closest verified update id, 
                                # this is because order are not permitted to skip a verified update . 
                                start_id = id + 1 
                                end_id = verified_buffer.next_verified_id(id) 
                                if end_id == -1 : 
                                    end_id = all_conntainers[-1]

                                if start_id > end_id :
                                    # this means that the orders is alredy upto date with 
                                    # the latest available modifications, we treat this like a 
                                    # modficication tick with no active windows : 
                                    foo1  , foo2 = modify_tick_with_no_active_windows(
                                        row=row, 
                                        near_ask=near_ask, 
                                        near_ask_id=near_ask_id, 
                                        quote=quote, 
                                        def_quote=def_quote, 
                                        total_quoting_orders=total_quoting_orders, 
                                        possible_quotes=possible_quotes , 
                                        counts = [ foo1 , foo2 ] , 
                                        time=time , f = f , 
                                        order_id_map=order_id_map 
                                    )
                                    order_id_matched = True 
                                    
                                # update total quotes 
                                
                                else : 
                                    if (dict_type is quote ) or ( dict_type is def_quote) : 
                                        reduce_total_quoting_orders(
                                            total_quoting_orders=total_quoting_orders,
                                            sp=dict_type[id][order_id]['sp'],
                                            qty=dict_type[id][order_id]['qty']
                                        )
                                        foo1 -= dict_type[id][order_id]['qty']
                                    
                                    for update_to_id in range(start_id,end_id+1) : 
                                        if not all_conntainers.book_id_exist( update_to_id ) : 
                                            continue 
                                        if (possible_dict := make_possible_quote_dict(row,near_ask,update_to_id)) is None : 
                                            continue 
                                        if dict_type is quote : 
                                            # find match : 
                                            if ( abs(dict_type[id][order_id]['sp'] - possible_dict['sp1']) ) <= beta*dict_type[id][order_id]['sp']/100 : 
                                                order_id_matched = True 
                                                # log the modiffed order : 
                                                dict_type[update_to_id][order_id] = make_quote_dict(possible_dict,dict_type[id][order_id]['num_updates'])
                                                foo2 += dict_type[update_to_id][order_id]['qty']
                                                # update the total order count 
                                                total_quoting_orders[dict_type[update_to_id][order_id]['sp']] =\
                                                    total_quoting_orders.get( dict_type[update_to_id][order_id]['sp'] , 0 ) + dict_type[update_to_id][order_id]['qty']
                                                foo1 += dict_type[update_to_id][order_id]['qty']
                                                # remove the previous order : 
                                                foo2 -= dict_type[id][order_id]['qty']
                                                del dict_type[id][order_id]
                                                f.write(f"{time},ModifyTickInWindow,{order_id},KeepQuoteOrder,{foo1},{foo2},{update_to_id}\n")
                                                break  
                                        elif dict_type is def_quote : 
                                            # find match : 
                                            if possible_dict['sp2'] is None : 
                                                continue 
                                            if ( abs(dict_type[id][order_id]['sp'] - possible_dict['sp2']) ) <= beta*dict_type[id][order_id]['sp']/100 : 
                                                order_id_matched = True 
                                                # log the modiffed order : 
                                                dict_type[update_to_id][order_id] = make_def_quote_dict(possible_dict,dict_type[id][order_id]['num_updates'])
                                                foo2 += dict_type[update_to_id][order_id]['qty']
                                                # update the total order count 
                                                total_quoting_orders[dict_type[update_to_id][order_id]['sp']] =\
                                                    total_quoting_orders.get( dict_type[update_to_id][order_id]['sp'] , 0 ) + dict_type[update_to_id][order_id]['qty']
                                                foo1 += dict_type[update_to_id][order_id]['qty']
                                                # remove the previous order : 
                                                foo2 -= dict_type[id][order_id]['qty']
                                                del dict_type[id][order_id]
                                                f.write(f"{time},ModifyTickInWindow,{order_id},KeepDefQuoteOrder,{foo1},{foo2},{update_to_id}\n")
                                                break
                                        else : 
                                            
                                            # remove the order from any containers that may have it :
                                            all_conntainers.remove_order_id(order_id)
                                            
                                            if dict_type[id][order_id]['sp1'] is not None : 
                                                if ( abs(dict_type[id][order_id]['sp1'] - possible_dict['sp1']) ) <= beta*dict_type[id][order_id]['sp1']/100 : 
                                                    order_id_matched = True 
                                                    # log the modiffed order : 
                                                    new_order_id = next(script_order_id_counter)
                                                    order_id_map[row['oid1']] = new_order_id
                                                    quote[update_to_id][new_order_id] = make_quote_dict(possible_dict,1)
                                                    foo2 += quote[update_to_id][new_order_id]['qty']
                                                    # update total spred counts : 
                                                    total_quoting_orders[quote[update_to_id][new_order_id]['sp']] =\
                                                        total_quoting_orders.get( quote[update_to_id][new_order_id]['sp'] , 0 ) +  quote[update_to_id][new_order_id]['qty']
                                                    foo1 += quote[update_to_id][new_order_id]['qty']
                                                    # remove the previous order : 
                                                    del dict_type[id][order_id]
                                                    f.write(f"{time},ModifyTickInWindow,{order_id},MovePossibletoQuote,{foo1},{foo2},{update_to_id}\n")
                                                    break
                                            elif (possible_dict['sp2'] is not None ) and (dict_type[id][order_id]['sp2'] is not None): 
                                                if ( abs(dict_type[id][order_id]['sp2'] - possible_dict['sp2']) ) <= beta*dict_type[id][order_id]['sp2']/100 : 
                                                    order_id_matched = True 
                                                    # log the modiffed order : 
                                                    new_order_id = next(script_order_id_counter)
                                                    order_id_map[row['oid1']] = new_order_id
                                                    def_quote[update_to_id][new_order_id] = make_def_quote_dict(possible_dict,1)
                                                    foo2 += def_quote[update_to_id][new_order_id]['qty']
                                                    # update total spred counts : 
                                                    total_quoting_orders[def_quote[update_to_id][new_order_id]['sp']] =\
                                                        total_quoting_orders.get( def_quote[update_to_id][new_order_id]['sp'] , 0 ) +  def_quote[update_to_id][new_order_id]['qty']
                                                    foo1 += def_quote[update_to_id][new_order_id]['qty']
                                                    # remove the previous order : 
                                                    del dict_type[id][order_id]
                                                    f.write(f"{time},ModifyTickInWindow,{order_id},MovePossibletoDefQuote,{foo1},{foo2},{update_to_id}\n")
                                                    break 
                
                                if order_id_found : 
                                    if not order_id_matched : 
                                        # just remove the modification : 
                                        if dict_type is quote : 
                                            foo2 -= dict_type[id][order_id]['qty']
                                            f.write(f"{time},ModifyTickInWindow,{order_id},RemoveQuoteOrder,{foo1},{foo2},{id}\n")
                                        elif dict_type is def_quote : 
                                            foo2 -= dict_type[id][order_id]['qty']
                                            f.write(f"{time},ModifyTickInWindow,{order_id},RemoveDefQuoteOrder,{foo1},{foo2},{id}\n")
                                        else :
                                            f.write(f"{time},ModifyTickInWindow,{order_id},RemovePossibleOrder,{foo1},{foo2},{id}\n")
                                        del dict_type[id][order_id]
                                        del order_id_map[row['oid1']]
                                    break 
                            
                            if order_id_found : 
                                break      
            
            if time >= TotalTime :  
                break 
            
            i += 1  
            
        if time >= TotalTime : 
            break 

with open( os.path.join( inputs['save_path'] , 'new_ticks_list.pkl' ) , 'wb') as f : 
    pickle.dump( new_ticks_list , f ) 