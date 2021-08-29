import glob
import os
def  remove_local_file ( filepath = './dags/Current/*.csv' ):
    files  =  glob . glob ( filepath )
    for  finish  in  files :
        os . remove ( finish )