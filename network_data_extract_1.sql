/****** SSMS의 SelectTopNRows 명령 스크립트 ******/
--data extraction 
--년도별 데이터 추출(2010-2013)  
--1단계: 경구제, 주사제, 단일제 약물 + 숫자로 시작하는 약물 코드만 추출
SELECT A.PERSON_ID, V.*
INTO KSY.DBO.T01_2011
FROM (
SELECT KEY_SEQ, GNL_NM_CD FROM [NHID].[dbo].[NHID_GY60_T1_2011] 
WHERE GNL_NM_CD  not LIKE '____00%'  AND ( GNL_NM_CD LIKE  '%A%' OR GNL_NM_CD LIKE  '%B%') AND GNL_NM_CD LIKE '[0-9]%') V
JOIN [NHID].[dbo].[NHID_GY20_T1_2011] A
ON A.KEY_SEQ = V.KEY_SEQ

--확인 SELECT TOP(100) * FROM KSY.DBO.T02_2012
--drop table KSY.DBO.T01_2011   select top(100) * from KSY.DBO.T01_0502 

--2단계: 한 청구번호 내 2개 이상약물 처방받은 경우만 추출
SELECT KEY_SEQ, COUNT(*) as CNT   INTO #list from KSY.DBO.T01_2011 group by KEY_SEQ 
--청구번호 내 처방약물 분포 확인 
SELECT CNT, COUNT(KEY_SEQ) AS CNT FROM #list group by CNT order by CNT desc 
--청구번호 1개 이상만 추출
SELECT * INTO #OVER1 FROM #list where CNT > 1
--DROP TABLE #list  DROP TABLE KSY.DBO.T02_2011
SELECT B.PERSON_ID, B.KEY_SEQ, B.GNL_NM_CD  INTO KSY.DBO.T02_2011
FROM KSY.DBO.T01_2011 B 
JOIN #OVER1 A
ON A.KEY_SEQ = B.KEY_SEQ
--drop table #OVER1
--SELECT TOP(100)* FROM KSY.DBO.T02_2011

--3단계: 성별, 분포 확인
SELECT DISTINCT  A.[PERSON_ID]
      ,A.[SEX]
      ,A.[AGE_GROUP]
  INTO KSY.DBO.SEXAGE_2011
  FROM [NHID].[dbo].[NHID_JK_2011] A 
  JOIN KSY.DBO.T02_2011 B
  ON A.PERSON_ID = B.PERSON_ID

-- 나이별, 성별 사람 수  2012년도 남: 65세 이상 46,413, 65세 이하 356,235 여자: 65세 이상 66,823 65세 이하 377,914 
select count(distinct PERSON_ID)
from (SELECT [PERSON_ID]
      ,[SEX]
      ,[AGE_GROUP]
  FROM [KSY].[dbo].[SEXAGE_2011]
  where AGE_GROUP < 14 
  )V
  where SEX =2 
--나이별 약물 정보 추출(65세 이하, 65세 이상) 
SELECT DISTINCT B.KEY_SEQ, LEFT( B.GNL_NM_CD, 4) AS GNL_NM_CD 
FROM 
( SELECT [PERSON_ID]
      ,[SEX]
      ,[AGE_GROUP]
  FROM [KSY].[dbo].[SEXAGE_2011]
  where AGE_GROUP < 14 
) V
JOIN [KSY].[dbo].[T02_2011] B 
ON V.PERSON_ID= B.PERSON_ID 
ORDER BY  B.KEY_SEQ

--확인 SELECT * FROM KSY.DBO.OVER65_0507 ORDER BY KEY_SEQ
-- drop table KSY.DBO.OVER65_0507

-- 질환별로 네트워크 구성(질환코드를 가진 환자와 t2 명단과 조인)
-- 당뇨  당뇨병(E10~E14), 질병코드는 한국표준질병사인분류(통계청)에 의함
-- 질환코드를 가진 환자와 t2 명단과 조인. 
-- 당뇨 성별에 따른 환자 수  #여성  30,021 # 남성 32,315
select count(distinct W.PERSON_ID) 
FROM 
(select DISTINCT A.PERSON_ID, A.KEY_sEQ, A.GNL_NM_CD
from [KSY].[dbo].[T02_2012] a
join nhid.[dbo].[NHID_GY20_T1_2012] b 
on a.PERSON_ID = b.PERSON_ID 
WHERE MAIN_SICK LIKE 'E10%' OR MAIN_SICK LIKE 'E11%' OR  MAIN_SICK LIKE 'E12%'   OR MAIN_SICK LIKE 'E13%' OR  MAIN_SICK LIKE 'E14%'
OR SUB_SICK LIKE  'E10%' OR SUB_SICK LIKE 'E11%' OR  SUB_SICK LIKE 'E12%'   OR SUB_SICK LIKE 'E13%' OR  SUB_SICK LIKE 'E14%'
) V
JOIN [KSY].[dbo].[SEXAGE_2012] W
ON W.PERSON_ID = V.PERSON_ID 
where W.SEX =1
-- 당뇨 나이 분포
select W.AGE_GROUP, COUNT(W.PERSON_ID) AS CNT   
FROM 
(select DISTINCT A.PERSON_ID, A.KEY_sEQ, A.GNL_NM_CD
from [KSY].[dbo].[T02_2012] a
join nhid.[dbo].[NHID_GY20_T1_2012] b 
on a.PERSON_ID = b.PERSON_ID 
WHERE MAIN_SICK LIKE 'E10%' OR MAIN_SICK LIKE 'E11%' OR  MAIN_SICK LIKE 'E12%'   OR MAIN_SICK LIKE 'E13%' OR  MAIN_SICK LIKE 'E14%'
OR SUB_SICK LIKE  'E10%' OR SUB_SICK LIKE 'E11%' OR  SUB_SICK LIKE 'E12%'   OR SUB_SICK LIKE 'E13%' OR  SUB_SICK LIKE 'E14%'
) V
JOIN [KSY].[dbo].[SEXAGE_2012] W
ON W.PERSON_ID = V.PERSON_ID 
GROUP BY W.AGE_GROUP 
ORDER BY AGE_gROUP
---당뇨 데이터 추출 
select DISTINCT A.KEY_sEQ, LEFT(A.GNL_NM_CD,4) AS GNL_NM_CD
from [KSY].[dbo].[T02_2012] a
join nhid.[dbo].[NHID_GY20_T1_2012] b 
on a.PERSON_ID = b.PERSON_ID 
WHERE MAIN_SICK LIKE 'E10%' OR MAIN_SICK LIKE 'E11%' OR  MAIN_SICK LIKE 'E12%'   OR MAIN_SICK LIKE 'E13%' OR  MAIN_SICK LIKE 'E14%'
OR SUB_SICK LIKE  'E10%' OR SUB_SICK LIKE 'E11%' OR  SUB_SICK LIKE 'E12%'   OR SUB_SICK LIKE 'E13%' OR  SUB_SICK LIKE 'E14%'

--고지질혈증 (E78%)
--고지혈증 성별에 따른 환자 수 (남 36635  여 46050) / 2012 남 33559, 여 42,540
select count(distinct W.PERSON_ID) 
FROM 
(select DISTINCT A.PERSON_ID, A.KEY_sEQ, A.GNL_NM_CD
from [KSY].[dbo].[T02_2012] a
join nhid.[dbo].[NHID_GY20_T1_2012] b 
on a.PERSON_ID = b.PERSON_ID 
WHERE MAIN_SICK LIKE 'E78%' OR SUB_SICK LIKE  'E78%' ) V
JOIN [KSY].[dbo].[SEXAGE_2012] W
ON W.PERSON_ID = V.PERSON_ID 
where W.SEX =2
--고지혈증 나이 분포  
select W.AGE_GROUP, COUNT(W.PERSON_ID) AS CNT   
FROM 
(select DISTINCT A.PERSON_ID, A.KEY_sEQ, A.GNL_NM_CD
from [KSY].[dbo].[T02_2012] a
join nhid.[dbo].[NHID_GY20_T1_2012] b 
on a.PERSON_ID = b.PERSON_ID 
WHERE MAIN_SICK LIKE 'E78%' OR SUB_SICK LIKE  'E78%' ) V
JOIN [KSY].[dbo].[SEXAGE_0502] W
ON W.PERSON_ID = V.PERSON_ID 
GROUP BY W.AGE_GROUP 
ORDER BY AGE_gROUP
---고지혈증 데이터 추출 
select DISTINCT A.KEY_sEQ, LEFT(A.GNL_NM_CD,4) AS GNL_NM_CD
from [KSY].[dbo].[T02_2012] a
join nhid.[dbo].[NHID_GY20_T1_2012] b 
on a.PERSON_ID = b.PERSON_ID 
WHERE MAIN_SICK LIKE 'E78%' OR SUB_SICK LIKE  'E78%' 

--고혈압 
--고혈압 성별에 따른 환자 수 (남자 66,160 여자 73,795), #2012 남자 63,936 여자 72,511
select count(distinct W.PERSON_ID) 
FROM 
(select DISTINCT A.PERSON_ID, A.KEY_sEQ, A.GNL_NM_CD
from [KSY].[dbo].[T02_2012] a
join nhid.[dbo].[NHID_GY20_T1_2012] b 
on a.PERSON_ID = b.PERSON_ID 
WHERE MAIN_SICK LIKE 'I1%' OR SUB_SICK LIKE  'I1%' ) V
JOIN [KSY].[dbo].[SEXAGE_2012] W
ON W.PERSON_ID = V.PERSON_ID 
where W.SEX =2
--고혈압 나이 분포  
select W.AGE_GROUP, COUNT(W.PERSON_ID) AS CNT   
FROM 
(select DISTINCT A.PERSON_ID, A.KEY_sEQ, A.GNL_NM_CD
from [KSY].[dbo].[T02_2012] a
join nhid.[dbo].[NHID_GY20_T1_2012] b 
on a.PERSON_ID = b.PERSON_ID 
WHERE MAIN_SICK LIKE 'I1%' OR SUB_SICK LIKE  'I1%' ) V
JOIN [KSY].[dbo].[SEXAGE_2012] W
ON W.PERSON_ID = V.PERSON_ID 
GROUP BY W.AGE_GROUP 
ORDER BY AGE_gROUP
---고혈압 데이터 추출 
select DISTINCT A.KEY_sEQ, LEFT(A.GNL_NM_CD,4) AS GNL_NM_CD
from [KSY].[dbo].[T02_2012] a
join nhid.[dbo].[NHID_GY20_T1_2012] b 
on a.PERSON_ID = b.PERSON_ID 
WHERE MAIN_SICK LIKE 'I1%' OR SUB_SICK LIKE  'I1%' 

--천식 (j45, j46) 
--천식  성별에 따른 환자 수 남자16,896/ 여자 20,162, 2012년 남자19372 여자 23221
select count(distinct W.PERSON_ID) 
FROM 
(select DISTINCT A.PERSON_ID, A.KEY_sEQ, A.GNL_NM_CD
from [KSY].[dbo].[T02_2012] a
join nhid.[dbo].[NHID_GY20_T1_2012] b 
on a.PERSON_ID = b.PERSON_ID 
WHERE MAIN_SICK LIKE 'J45%' OR SUB_SICK LIKE  'J46%' ) V
JOIN [KSY].[dbo].[SEXAGE_2012] W
ON W.PERSON_ID = V.PERSON_ID 
where W.SEX =1
--천식 나이 분포  
select W.AGE_GROUP, COUNT(W.PERSON_ID) AS CNT   
FROM 
(select DISTINCT A.PERSON_ID, A.KEY_sEQ, A.GNL_NM_CD
from [KSY].[dbo].[T02_2012] a
join nhid.[dbo].[NHID_GY20_T1_2012] b 
on a.PERSON_ID = b.PERSON_ID 
WHERE MAIN_SICK LIKE 'J45%' OR SUB_SICK LIKE  'J46%' ) V
JOIN [KSY].[dbo].[SEXAGE_2012] W
ON W.PERSON_ID = V.PERSON_ID 
GROUP BY W.AGE_GROUP 
ORDER BY AGE_gROUP
---천식  데이터 추출 
select DISTINCT A.KEY_sEQ, LEFT(A.GNL_NM_CD,4) AS GNL_NM_CD
from [KSY].[dbo].[T02_2012] a
join nhid.[dbo].[NHID_GY20_T1_2012] b 
on a.PERSON_ID = b.PERSON_ID 
WHERE MAIN_SICK LIKE 'J45%' OR SUB_SICK LIKE  'J46%' 


--류마티스 관절염 (m05, m06) 
--류마티스 관절염  성별에 따른 환자 수 남자 1,622/ 여자 4,344 / 2012년 남자 1462,  여자 4215
select count(distinct W.PERSON_ID) 
FROM 
(select DISTINCT A.PERSON_ID, A.KEY_sEQ, A.GNL_NM_CD
from [KSY].[dbo].[T02_2012] a
join nhid.[dbo].[NHID_GY20_T1_2012] b 
on a.PERSON_ID = b.PERSON_ID 
WHERE MAIN_SICK LIKE 'M05%' OR SUB_SICK LIKE  'M06%' ) V
JOIN [KSY].[dbo].[SEXAGE_2012] W
ON W.PERSON_ID = V.PERSON_ID 
where W.SEX =1
--류마티스관절염 나이 분포  
select W.AGE_GROUP, COUNT(W.PERSON_ID) AS CNT   
FROM 
(select DISTINCT A.PERSON_ID, A.KEY_sEQ, A.GNL_NM_CD
from [KSY].[dbo].[T02_2012] a
join nhid.[dbo].[NHID_GY20_T1_2012] b 
on a.PERSON_ID = b.PERSON_ID 
WHERE MAIN_SICK LIKE 'M05%' OR SUB_SICK LIKE  'M06%' ) V
JOIN [KSY].[dbo].[SEXAGE_2012] W
ON W.PERSON_ID = V.PERSON_ID 
GROUP BY W.AGE_GROUP 
ORDER BY AGE_gROUP
---류마티스  데이터 추출 
select DISTINCT A.KEY_sEQ, LEFT(A.GNL_NM_CD,4) AS GNL_NM_CD
from [KSY].[dbo].[T02_2012] a
join nhid.[dbo].[NHID_GY20_T1_2012] b 
on a.PERSON_ID = b.PERSON_ID 
WHERE MAIN_SICK LIKE 'M05%' OR SUB_SICK LIKE  'M06%'
