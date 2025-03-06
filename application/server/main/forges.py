import re
import pandas as pd
from application.server.main.logger import get_logger

logger = get_logger(__name__)

def build_rgx_forges():
    forges = pd.read_csv('forges.csv', header=None)[0].to_list()
    forges = [f.replace('/', '\/').replace('.', '\.') for f in forges]
    forges_rgx = [r"\b" + f + r"\/[\w\-\.]+(?:\/[\w\-\.]+)*\b" for f in forges] 
    rgx = re.compile('|'.join(forges_rgx))
    return rgx

FORGE_RGX = build_rgx_forges()

def detect_forge(s):
    return list(set(re.findall(FORGE_RGX, s)))
