import pandas as pd

url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQbXdwATZriOICgr1wEN7gxfdQug2pH43HxTisB6TfTA0jOCOQCE6qB2Yjv7X1U7IDQlh0Qp_Kl-j_T/pub?gid=1973345800&single=true&output=csv"

df = pd.read_csv(url)

print(df.head())