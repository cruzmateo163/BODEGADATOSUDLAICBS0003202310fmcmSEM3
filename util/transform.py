
from transform.transform_channels import tra_channels
from transform.transform_countries import tra_countries
from transform.transform_promotions import tra_promotions

def ex_tra(etl_process):
    tra_channels(etl_process)
    tra_countries(etl_process)
    tra_promotions(etl_process)


 
    