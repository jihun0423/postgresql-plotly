# postgresql-plotly

sql 학습을 최근에 너무 하지 않은탓에 대부분 잊어버린 것 같아 이번에는 postgresql을 이용해보기로 하였다.
이전에는 oracle이나 pyspark를 사용해서 해보았지만 이번에는 postgresql을 jupyter notebook과 연동하여 사용해보기로 한다. 
시각화는 이전에 사용하였던 plotly를 복습겸 사용하고자 한다.  

사실 이전에 사용하였던 pyspark도 복습겸 사용해보고 싶었으나, postgresql과 pyspark연동이 계속 오류때문에 되지 않는다.
사용할 데이터셋은 North-Wind 상거래 데이터셋과 구글 아날리틱스 세션 로그 데이터셋이다. 
로그 데이터 분석에도 마침 흥미가 있었기에 (빅콘테스트 핀다 데이터 분석대회때 로그데이터를 접해본 적이 있지만, 일정 수만큼의 데이터만이 제공되어 있어 분석 모델에 넣기는 부적합하다는 판단하에 단순 참고용으로만 살펴본 기억이 있다.) 


* postgresql만 사용하다보니 코딩테스트 연습을 할 때 막히는 경우가 좀 있었다... 아무래도 postgresql에만 있는 함수가 꽤나 편의성이 높아서 익숙해진 탓 인것 같다.

## 학습 로그

* 가중이동평균 (WMA)를 구하는것에 상당히 애를 먹었다. 월별이나 일별 매출을 구한 뒤 단순 이동평균을 구하는 것은 쉬웠으나, 더 가까운 일자에 더 큰 가중치를 주는 가중이동평균은 sql만으로 하기에는 굉장히 번거로웠다. (매출 테이블을 원하는 기간만큼 추출하기 위한 셀프조인, 그 기간들에 가중치를 적용하기 위한 case when 등)
그래서 월별매출 테이블을 pandas와 연동 후 가중 이동평균을 구하는 방법을 찾아보았지만, 덜 복잡하긴 하지만 여전히 번거로웠다. (rolling을 통해 기간만큼 추출뒤 apply를 통해 weight 부여)

* fanchart라고 하는 그래프를 만들기 위하여 쿼리를 작성하던 중, first_value() 라는 함수를 알게되었다. 그동안 그룹별로 처음 값을 구하기 위해 row_number을 파티션으로 나누고 1인값을 불러오는 식으로 작성을 해왔었는데, first_value에 기존 윈도우 함수를 작성하듯이 파티션별로 나누면 바로 구할 수 있다는 걸 알게되었다. 

* 보통 피벗테이블을 만들고자 한다면, 데이터를 불러온 뒤 파이썬의 판다스에서 group by나 pivot_table을 통하여 만들어 왔었는데, 문득 sql을 이용해서는 어떻게 만드는지 궁금해져서 알아보았다. group by를 한 뒤에 집계 함수를 사용할 때 case when을 사용하면 되는 것이였다. (여전히 파이썬이 더 편하다)

* sql 학습을 마치고 공부할 예정인 추천 알고리즘의 일부분을 sql로 구현해 보았다. 어떤 특정한 상품을 주문했을 때 같이 주문된 상품들중 빈도수가 가장 많은 제품을 골라내는 쿼리와, 특정 상품을 주문했을 때 그 유저가 같이 주문했던 제품들중 빈도수가 가장 많은 제품을 골라내는 쿼리를 작성하였다. 

* RFM 분석에 대하여 학습을 하였다. RFM 분석은 고객이 얼마나 최근에 (Recency), 얼마나 자주 (Frequency), 얼마나 많은 금액 (Monetary)를 사용했는지에 따라 고객을 분류하는 기법으로, 이전에 오퍼레이션스 매니지먼트라는 경영학과 과목을 수강했을 때 배운적이 있던 내용이였다. 이 과목에서 배웠던 내용으로는 기업에서 신규 소비자를 끌어들이는데 필요한 비용은 기존 고객들을 유지하는 비용보다 매우 많은 비용이 소모되므로 기존 고객들을 유지시킬 수 있는 방법을 모색하는 것이 중요하다는 것을 배웠었는데, 여기서 RFM분석을 통하여 이전에는 자주 높은 금액으로 주문한 고객들을 찾아내어 혜택같은 것을 줌으로써 다시 이 기업의 소비자로 돌아오게 한다던지 하는 방법을 사용할 수 있다.

* 가장 최근의 주문으로부터 며칠이 지났는지 구한 뒤에, 이 날짜들을 5분위로 나누어 frequency 등급을 측정해주었다. 하지만 최근에만 주문을 덜 했을 뿐이지, 평소에 주문을 매우 빈번하게 한 경우도 있을 것이라고 생각하여 그 이전에 다음 주문들까지의 평균 기간들을 구한 뒤 역시 분위로 나누었다.   
어차피 머신러닝이나 딥러닝에서 이런 데이터들을 사용할 때에는 모델에서 학습 과정중에 알아서 등급으로 분류하거나 특징들을 추출할테니 의미는 없을 수도 있지만, 그 이전에 직관성을 높이기 위해 시각화를 하기 위한 준비 과정이라 생각하고 코드를 작성해 보았다. 처음에는 ntile을 이용하여 간단하게 등급을 분류하고자 하였으나, 바로 문제가 발생하였다.  
Frequency Rank를 부여하는 과정에서 현재 사용하고 있는 데이터의 기간이 2달 정도로 짧기 때문에 (용량 관계상),  대부분의 고객들의 주문 횟수는 1번이나 2번에서 그쳤고 그에 따라 ntile을 쓰면 ntile은 같은 크기의 순위 그룹으로 분류하는 것이기 때문에 매우 극단적으로 분류가 될 뿐더러, 같은 주문횟수인데도 다른 등급으로 분류되는 불상사도 생기게 된다.   
따라서 ntile을 이용하는 방법은 배제하고, 서브쿼리를 통하여 recency, frequency, monetary 각각에 대한 등급 기준 표를 만들고 이에 따라 분류하기로 하였다.  
그 뒤 전체 등급을 배정한 뒤, 트리맵을 통하여 분포를 파악하기 쉽도록 하였다.

* 주문 데이터에 이어서 로그 데이터를 분석해 보기로 하였다. 이전에 참여했던 대회에서도 로그 데이터를 만져볼 기회는 있었지만, 그 때 당시 로그 데이터는 모든 데이터를 제공 받은게 아니라 일정 분량의 데이터만 제공 받았었기에 분석 도중 머신러닝 모델에 활용하기에는 적합하지 않다고 판단하여 참고용으로만 사용한 기억이 있다.
웹 프로그래밍을 해보았거나 학습을 한 경험이 없기에 가장 기초적인 내용을 먼저 학습을 하였다. 유저가 웹페이지에 접속을 할 때 세션이 생성이 된 후 일정 시간이 지나면 세션이 끝나게 되고, 다음에 접속을 할 때 또 세션이 생성되는 방식인 것 같았다. 즉, 유저별로 세션은 여러개 있을 수 있고, 각 세션별로 한 행동들이 sess_hits라는 테이블에 따로 저장이 되어있었다.

* dau, wau, mau를 구하되 사용자가 지정한 날짜를 기준으로 구하는 방법에 대하여 학습을 하였다. 이전에 하던데로 date_trunc를 이용하여 쉽게 구할 수 있지만, 이건 데이터가 기본적으로 쌓여있는 곳에서 정해진 기간 틀에서 구하는 방식이라 문제가 있다. (wau를 구할때의 예시로 들어보면, 일요일부터 다음주 일요일 전까지의 유저 수를 구하거나, 혹은 어떠한 날짜가 주어졌을 때 그 날짜까지 일주일동안의 유저 수를 구하는 것을 date_trunc로는 불가능하다.) 따라서, 바인드 변수 (:column)을 이용하여 원하는 날짜를 입력받고 interval을 통하여 wau나 mau를 구하는 방식을 사용하였다.

* postgresql에서 do 명령어와 for loop를 이용하여 날짜에 따른 dau,wau,mau를 insert하는 쿼리를 만들어보았다. for loop에 날짜를 주고, 이 날짜마다 dau등을 구한 뒤에 insert values를 해주는 식으로 만들었는데, 속도도 느리고 쿼리도 복잡해서 다른 방법을 찾는 것이 더 나을 것 같다는 생각이 들었다.
recursive문을 통하여 임시 view를 만든 뒤 insert를 하는 방법도 생각해보았으나 오히려 더 복잡해지고, 시간은 그대로일 것 같았다.
더 좋은 방법을 찾아내었는데, generate_series를 통해 모든 날짜 리스트를 만들고, 이 날짜 리스트와 세션들을 cross join을 한 뒤 where에서 구하고 싶은 조건 (날짜 기준 몇일 전까지의 데이터만 불러오는)을 설정해준다면 단 한번의 join으로 모든 날짜에 대하여 for loop를 사용한 것처럼 결과가 나오게 된다.  

* 특정 날짜에서 최근 한달간의 접속자들중, 신규 생성된 유저 (한달 이내) 수와 기존 유저 (생성 날짜가 한달보다 오래된)수를 구해보았다. 현재 사용하고 있는 데이터에서는 신규 생성 유저는 매일 매일 증가하고 있어 좋은 모습을 보이지만, 기존 유저의 수는 상승폭이 적어 체리피커가 많은 사이트 인 것 같다. 

* 이전까지 했던 분석들은 대부분 날짜 (계정 생성 시간, 주문 시간, 방문 시간 등)을 이용하였다면, 이번에는 이용 경로에 대해 분석해보고자 한다. 직접 검색해서 온 경우, 광고를 통해 온 경우 등의 상황에 따라 신규유저/기존유저의 수를 본다거나, 유입 경로에 따라 주문은 얼마나 나왔으며 매출은 얼마나 나왔는지 등 분석해 볼 예정이다.

* 세션별 어떤 동작 (hit)가 있었는지 알아보기 위해 sess_hits 테이블을 불러온 뒤, 분석을 해 보았다. 처음에는 가장 단순하게 세션별로 동작이 몇번 있었는지 분포를 알아 보았는데, 대부분 10번 전후정도인데 최대 횟수가 500이기때문에 분포가 치우치기 때문에 일정 범위 이상 벗어난 값들 (q3+1.5*IQR 이상)을 제거해 준 뒤 분포를 다시 파악하였다.

* 페이지별 평균 세션 수와 평균 접속 시간을 알아보는 쿼리를 작성하였다.

* 유저별 세션의 마지막 페이지 (종료 페이지)를 구하기 위한 쿼리 작성 중 막히던 부분이 있어 고민하던 중 해결하였는데, 이를 통해 윈도우 함수와 좀 더 친해져야겠다는 생각이 들었다. partition by에 2개의 컬럼을 한번에 사용할 생각을 하지 못했어서, 하나의 컬럼만 파티션으로 나누고 임시 뷰를 만든 뒤 나머지 하나의 컬럼으로 또 파티션으로 나누는 비효율적인 방법을 생각하다가, 검색을 해보니 여러개의 컬럼으로도 된다는 것을 알아냈다. 

* 페이지별 바운스 유저 수(페이지 접속 후 바로 종료)와 특정 페이지로 시작하였지만 다른 페이지로 넘어간 뒤 종료하는 유저수를 구한 뒤 페이지별 이탈율을 구하였는데, 이 데이터셋의 경우에는 홈페이지에서 이탈율이 50%가 넘는 현상이 나타났다. 홈페이지의 로딩이 느리다거나, UI가 너무 산만하다던지 하는 요인으로 인하여 다른 페이지를 클릭할 생각도 하지 않고 바로 종료버튼을 누르는 유저가 50%가 넘는다는 뜻이다. 
추가로 궁금하여 접속한 경로별로 다른 경향이 있지 않을까 분석해본 결과, 어느정도 차이는 있지만 직접 경로를 입력하여 온 유저들도 40%가 넘는걸 보면 홈페이지의 개편이 필수인 사이트인 것 같다.

* 페이지별 종료율도 조사해본 결과 종료율이 매우 높은 (>70%) 페이지중 특징적이였던 점이, 역시나 홈페이지에서 종료율이 매우 높았고, 로그인 화면에서의 종료율도 매우 높았다. 로그인 과정에서 아이디나 비밀번호가 기억이 나지않아 그대로 닫는 경우던지, 무언가를 하려고 하는데 로그인을 요구하여 닫는날 경우가 꽤나 잦은것 같다. 또 당연하게도, 상품 주문 완료 페이지에서의 종료율이 매우 높았다. 주문 완료 후 다른 페이지로 넘어가는 경우는 20%도 되지 않았다.

* 어떤 특정한 날짜에 생성된 유저들 중에서 일정 기간동안 남아 있었는지 (재접속을 하였는지), 즉 잔존율을 구하는 쿼리를 작성해 보았다. 바운드 변수를 이용해서 구하고 싶은 날짜를 정한 뒤, 그 날짜로부터 n일이 지났을때 그 이후에 유저가 접속한 기록이 있는지를 통하여 잔존율을 계산하였다. 즉, 유저의 세션 데이터의 마지막 날짜를 가져온 뒤 이를 바탕으로 유저의 이탈 여부를 계산한 것이다.
그러나 위의 쿼리의 문제점은, 단순히 접속한 날짜만을 이용한 것이기에 우연히 다시 접속을 하게 된건지 이 사이트를 계속해서 꾸준히 사용해온 것인지에 대해서는 분류할 방법이 없다는 것이다. 이를 해결하기 위해 세션별로 한 행동들의 데이터를 추가로 조인하여 구해보고자 한다.
페이지 접속 뿐만이 아니라 적어도 다른 하나의 행동이라도 한 유저들의 마지막 액티브 날짜를 구한 뒤, 전처럼 잔존율을 구해보았다. 단순히 접속 일자만을 기준으로 구한 잔존율보다 훨씬 줄어든 것을 확인할 수 있었다.

* 잔존율을 좀 더 빠르고 깔끔하게 구하는 방법을 생각해내어 쿼리를 작성해 보았다. 이전에 RFM 분석때 사용했던 방법처럼, 먼저 generate_series (오라클에서 사용하던 connect by level을 사용하고자 하였으나 postgresql은 지원 x)을 이용하여 생성시간 컬럼을 따로 만들어준 뒤, 유저 데이터와 cross join을 하되 생성시간이 일치하는 컬럼들만 남기도록 where절을 사용하였다. 이렇게 되면 generate_series로 만든 생성시간 컬럼에 의해 마치 groupby를 한 것처럼 그 값에 해당하는 값들만 남게되고, 나머지 값들은 사라져 용량도 줄어들고 매번 날짜별로 구할 필요가 없어 속도도 빨라지며 원하는 값을 구하도록 쿼리를 작성하는데 매우 편리해진다.
그리고 잔존율을 구하는 방법을 바꾸었는데, 마지막 접속일을 기준으로 하는 것이 아닌 단순히 그 n일 뒤에 접속을 하였는지로 구하는 것이 잔존율이라고 한다. 즉 이전에 구했던 잔존율?보다 훨씬 식이 단순해진다.
주별, 채널별 잔존율도 구해 보았으나, 3달치의 데이터만 가지고 분석을 하는 것이다 보니 데이터의 양이 부족하다는 느낌이 좀 들었다.

* 일별 매출 전환율과 매출액을 구해보았다. 처음에 너무 한번에 구하려고 하다가 매출액 부분에서 중복 값이 들어가 잘못 구하게 되었다. 차근차근 임시 쿼리를 생성해가며 일별 전환율과 일별 매출을 최종적으로 조인하여 문제를 해결하였다. 이런 쇼핑몰에서는 전환율이 어느정도여야 좋은건지는 잘 모르겠지만, 일단 이 데이터에서는 일별 전환율이 0~3% 사이를 유지하였다. 
 
