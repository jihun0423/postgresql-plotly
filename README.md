# postgresql-plotly

sql 학습을 최근에 너무 하지 않은탓에 대부분 잊어버린 것 같아 이번에는 postgresql을 이용해보기로 하였다.
이전에는 oracle이나 pyspark를 사용해서 해보았지만 이번에는 postgresql을 jupyter notebook과 연동하여 사용해보기로 한다. 
시각화는 이전에 사용하였던 plotly를 복습겸 사용하고자 한다.

사실 이전에 사용하였던 pyspark도 복습겸 사용해보고 싶었으나, postgresql과 pyspark연동이 계속 오류때문에 되지 않는다.
사용할 데이터셋은 North-Wind 상거래 데이터셋이다

가중이동평균 (WMA)를 구하는것에 상당히 애를 먹었다. 월별이나 일별 매출을 구한 뒤 단순 이동평균을 구하는 것은 쉬웠으나, 더 가까운 일자에 더 큰 가중치를 주는 가중이동평균은 sql만으로 하기에는 굉장히 번거로웠다. (매출 테이블을 원하는 기간만큼 추출하기 위한 셀프조인, 그 기간들에 가중치를 적용하기 위한 casewhen 등)
그래서 월별매출 테이블을 pandas와 연동 후 가중 이동평균을 구하는 방법을 찾아보았지만, 덜 복잡하긴 하지만 여전히 번거로웠다. (rolling을 통해 기간만큼 추출뒤 apply를 통해 weight 부여)

fanchart라고 하는 그래프를 만들기 위하여 쿼리를 작성하던 중, first_value() 라는 함수를 알게되었다. 그동안 그룹별로 처음 값을 구하기 위해 row_number을 파티션으로 나누고 1인값을 불러오는 식으로 작성을 해왔었는데, first_value에 기존 윈도우 함수를 작성하듯이 파티션별로 나누면 바로 구할 수 있다는 걸 알게되었다. 
