
###############################################################
# RFM ile Müşteri Segmentasyonu (Customer Segmentation with RFM)
###############################################################

###############################################################
# İş Problemi (Business Problem)
###############################################################
# FLO müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama stratejileri belirlemek istiyor.
# Buna yönelik olarak müşterilerin davranışları tanımlanacak ve bu davranış öbeklenmelerine göre gruplar oluşturulacak..

###############################################################
# Veri Seti Hikayesi
###############################################################

# Veri seti son alışverişlerini 2020 - 2021 yıllarında OmniChannel(hem online hem offline alışveriş yapan) olarak yapan müşterilerin geçmiş alışveriş davranışlarından
# elde edilen bilgilerden oluşmaktadır.

# master_id: Eşsiz müşteri numarası
# order_channel : Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile, Offline)
# last_order_channel : En son alışverişin yapıldığı kanal
# first_order_date : Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date : Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online : Muşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline : Muşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online : Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline : Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline : Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online : Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12 : Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

###############################################################
# GÖREVLER
###############################################################

# GÖREV 1: Veriyi Anlama (Data Understanding) ve Hazırlama


# 1. flo_data_20K.csv verisini okuyunuz.
import datetime as dt
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x:'%5f'%x)
df_=pd.read_csv('HAFTA 3 ODEV/ODEV 1/flo_data_20k.csv')
df=df_.copy()
df.head()
#                              master_id order_channel last_order_channel  \
#0  cc294636-19f0-11eb-8d74-000d3a38a36f   Android App            Offline
#1  f431bd5a-ab7b-11e9-a2fc-000d3a38a36f   Android App             Mobile
#2  69b69676-1a40-11ea-941b-000d3a38a36f   Android App        Android App
#3  1854e56c-491f-11eb-806e-000d3a38a36f   Android App        Android App
#4  d6ea1074-f1f5-11e9-9346-000d3a38a36f       Desktop            Desktop
#  first_order_date last_order_date last_order_date_online  \
#0       2020-10-30      2021-02-26             2021-02-21
#1       2017-02-08      2021-02-16             2021-02-16
#2       2019-11-27      2020-11-27             2020-11-27
#3       2021-01-06      2021-01-17             2021-01-17
#4       2019-08-03      2021-03-07             2021-03-07
#  last_order_date_offline  order_num_total_ever_online  \
#0              2021-02-26                     4.000000
#1              2020-01-10                    19.000000
#2              2019-12-01                     3.000000
#3              2021-01-06                     1.000000
#4              2019-08-03                     1.000000
#   order_num_total_ever_offline  customer_value_total_ever_offline  \
#0                      1.000000                         139.990000
#1                      2.000000                         159.970000
#2                      2.000000                         189.970000
#3                      1.000000                          39.990000
#4                      1.000000                          49.990000
#   customer_value_total_ever_online       interested_in_categories_12
#0                        799.380000                           [KADIN]
#1                       1853.580000  [ERKEK, COCUK, KADIN, AKTIFSPOR]
#2                        395.350000                    [ERKEK, KADIN]
#3                         81.980000               [AKTIFCOCUK, COCUK]
#4                        159.990000                       [AKTIFSPOR]





# 2. Veri setinde
                     # a. İlk 10 gözlem,
df.head(10)

                     # b. Değişken isimleri,
df.columns
#Index(['master_id', 'order_channel', 'last_order_channel', 'first_order_date',
#       'last_order_date', 'last_order_date_online', 'last_order_date_offline',
#       'order_num_total_ever_online', 'order_num_total_ever_offline',
#       'customer_value_total_ever_offline', 'customer_value_total_ever_online',
#       'interested_in_categories_12'],
#      dtype='object')



                     # c. Betimsel istatistik,
df.describe().T

                     # d. Boş değer,
df.isnull().values.any() #False , yani hiç boş değer yok.
df.isnull().sum() #bu koda da gerek kalmaz

                     # e. Değişken tipleri, incelemesi yapınız.




# 3. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Her bir müşterinin toplam
# alışveriş sayısı ve harcaması için yeni değişkenler oluşturun.

df["total_order"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
df["total_value"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]

#4. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
df.dtypes
#master_id                             object
#order_channel                         object
#last_order_channel                    object
#first_order_date                      object
#last_order_date                       object
#last_order_date_online                object
#last_order_date_offline               object
#order_num_total_ever_online          float64
#order_num_total_ever_offline         float64
#customer_value_total_ever_offline    float64
#customer_value_total_ever_online     float64
#interested_in_categories_12           object
#dtype: object


for col in df.columns:     #zaman bilgisi içeren değişkenlerin tip bilgilerinin datetime olması gerekiyor. RFM analizi için.
    if "date" in col:
        df[col] = pd.to_datetime(df[col])


#5. Alışveriş kanallarındaki müşteri sayısının, ortalama alınan ürün sayısının ve ortalama harcamaların dağılımına bakınız.

df.groupby("order_channel").agg({"total_order": ["count","mean"],
                                "total_value": ["count","mean"]})


#6. En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.

df[["master_id", "total_value"]].sort_values(by="total_value", ascending=False).head(10)

#7. En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
df.sort_values("total_order",ascending=False).head(10)


#8. Veri ön hazırlık sürecini fonksiyonlaştırınız.
def create_rfm(dataframe):
    dataframe["total_order"] = dataframe["order_num_total_ever_online"]+dataframe["order_num_total_ever_offline"]
    dataframe["total_value"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]

    for col in dataframe.columns:
        if "date" in col:
            dataframe[col] = pd.to_datetime(dataframe[col])

    return dataframe

df = create_rfm(df)


# GÖREV 2: RFM Metriklerinin Hesaplanması

df.head()

df["last_order_date"].max()  #son sipariş tarihini görürüz. Analizi bugun üzernden yapmak saçma olacaktır. son satın alma tarihinden 2 gün sonrası için yapalım.
today_date = dt.datetime(2021, 6, 1)
type(today_date)

rfm=df.agg({'master_id': lambda master_id: master_id,
            'last_order_date': lambda last_order_date: (today_date - last_order_date).days,
            'total_order': lambda total_order: total_order,
            'total_value': lambda total_value:  total_value,
            'interested_in_categories_12': lambda interested_in_categories_12: interested_in_categories_12})


rfm.head()

rfm.columns= ["master_id", "recency", "frequency", "monetary","interested_in_categories_12"]

# GÖREV 3: RF ve RFM Skorlarının Hesaplanması

rfm["recency_score"]= pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])
rfm["frequency_score"] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])  #rank(method="first") eklememiz gerekti. ilk gordugunun ilk değere ata
rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])

rfm["frequency"].value_counts()
rfm["frequency_score"].value_counts()

rfm["recency_score"].value_counts()

# GÖREV 4: RF Skorlarının Segment Olarak Tanımlanması

rfm["RF_SCORE"] = (rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str))
#regex
seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalist',
    r'5[4-5]': 'champions'
}

rfm["segment"] = rfm["RF_SCORE"].replace(seg_map, regex=True)
rfm.head()

# GÖREV 5: Aksiyon zamanı!
           # 1. Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.
           # 2. RFM analizi yardımı ile 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv ye kaydediniz.
                   # a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri tercihlerinin üstünde. Bu nedenle markanın
                   # tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak iletişime geçeilmek isteniliyor. Sadık müşterilerinden(champions,loyal_customers),
                   # ortalama 250 TL üzeri ve kadın kategorisinden alışveriş yapan kişiler özel olarak iletişim kuralacak müşteriler. Bu müşterilerin id numaralarını csv dosyasına
                   # yeni_marka_hedef_müşteri_id.cvs olarak kaydediniz.
                   # b. Erkek ve Çoçuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte iyi müşteri olan ama uzun süredir
                   # alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve yeni gelen müşteriler özel olarak hedef alınmak isteniliyor. Uygun profildeki müşterilerin id'lerini csv dosyasına indirim_hedef_müşteri_ids.csv
                   # olarak kaydediniz.
rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean"])

rfm.to_csv("rfm_flo.csv")

#1.)

hedef1df = rfm[["master_id", "segment", "interested_in_categories_12","monetary"]]


hedef1df = hedef1df.loc[(hedef1df["interested_in_categories_12"].str.contains("KADIN")) & (hedef1df["monetary"] > 250) &
                      ((hedef1df["segment"] == "loyal_customers") | (hedef1df["segment"] == "champions"))]

hedef1df.head()

hedef1df.to_csv("yeni_marka_hedef_müşteri_id.csv")


#2.)

boys_40df = rfm[["master_id", "segment", "interested_in_categories_12"]]


boys_40df = boys_40df.loc[((boys_40df["interested_in_categories_12"].str.contains("COCUK")) |
                           (boys_40df["interested_in_categories_12"].str.contains("ERKEK"))) &
                           ((boys_40df["segment"] == "hibernating") |
                           (boys_40df["segment"] == "cant_loose") |
                           (boys_40df["segment"] == "new_customers"))]

boys_40df.head()


boys_40df.to_csv("indirim_hedef_müşteri.csv")




# GÖREV 6: Tüm süreci fonksiyonlaştırınız.

def create_rfm(dataframe, csv=False):

    #veriyi hazırlama
    dataframe["total_order"] = dataframe["order_num_total_ever_online"]+dataframe["order_num_total_ever_offline"]
    dataframe["total_value"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]

    for col in dataframe.columns:
        if "date" in col:
            dataframe[col] = pd.to_datetime(dataframe[col])



   #RFM metrikleri hesaplanması

    today_date = dt.datetime(2021, 6, 1)
    rfm = df.agg({'master_id': lambda master_id: master_id,
                  'last_order_date': lambda last_order_date: (today_date - last_order_date).days,
                  'total_order': lambda total_order: total_order,
                  'total_value': lambda total_value: total_value,
                  'interested_in_categories_12': lambda interested_in_categories_12: interested_in_categories_12})

    rfm.columns = ["master_id", "recency", "frequency", "monetary", "interested_in_categories_12"]


   #RFM skorlarının hesaplanması

    rfm["recency_score"] = pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])
    rfm["frequency_score"] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4,
                                                                                       5])  # rank(method="first") eklememiz gerekti. ilk gordugunun ilk değere ata
    rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])

    rfm["RF_SCORE"] = (rfm["recency_score"].astype(str) + rfm["frequency_score"].astype(str))

    #Segmentlerin İsimlendirilmesi
    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalist',
        r'5[4-5]': 'champions'
    }

    rfm["segment"] = rfm["RF_SCORE"].replace(seg_map, regex=True)

    if csv:
        rfm.to_csv("rfm.csv")

    return rfm

rfm_new = create_rfm(df, csv=True)
