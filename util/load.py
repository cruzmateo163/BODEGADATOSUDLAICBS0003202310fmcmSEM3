from load.load_channels import load_channels
from load.load_countries import load_countries
from load.load_customers import load_customers
from load.load_products import load_products
from load.load_promotions import load_promotions
from load.load_sales import load_sales
from load.load_times import load_times

def load_pro(etl_process):
    load_channels(etl_process)
    load_countries(etl_process)
    load_products(etl_process)
    load_promotions(etl_process)
    load_times(etl_process)
    load_sales(etl_process)

    


   