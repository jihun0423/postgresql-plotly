# postgresql-plotly

sql 학습을 최근에 너무 하지 않은탓에 대부분 잊어버린 것 같아 이번에는 postgresql을 이용해보기로 하였다.
이전에는 oracle이나 pyspark를 사용해서 해보았지만 이번에는 postgresql을 jupyter notebook과 연동하여 사용해보기로 한다. 
시각화는 이전에 사용하였던 plotly를 복습겸 사용하고자 한다.

사실 이전에 사용하였던 pyspark도 복습겸 사용해보고 싶었으나, postgresql과 pyspark연동이 계속 오류때문에 되지 않는다.
사용할 데이터셋은 North-Wind 상거래 데이터셋이다

가중이동평균 (WMA)를 구하는것에 상당히 애를 먹었다. 월별이나 일별 매출을 구한 뒤 단순 이동평균을 구하는 것은 쉬웠으나, 더 가까운 일자에 더 큰 가중치를 주는 가중이동평균은 sql만으로 하기에는 굉장히 번거로웠다. 
