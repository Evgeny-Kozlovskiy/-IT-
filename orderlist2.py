import os

import mysql.connector
import decimal
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="6666"
)
cur = conn.cursor()

# использование созданной БД
cur.execute("""USE OrderLog;""")

# считываем тикеры из файла
tickers_text = open('ListingSecurityList.csv', encoding = "cp1251").read()
tickers = {}
for elem in tickers_text.split('\n')[:-1]:
    t = elem.split(',')
    tickers[t[7].replace("\"", "")] = t[5].replace("\"", "")

ticker = input("Введите тикер: ")
# пример формата времени: 101000000 (10 часов 10 минут)
time = int(input("Введите время: "))

# в зависимости от типа инструмента выгружаем информацию о заявках до указанного момента времени
if tickers[ticker] == 'Акция обыкновенная':
        t = f"""SELECT orderno, action, buysell, price, volume, time FROM CommonStock
where TIME<= '%s' and seccode = '%s';""" % (time, ticker)
        cur.execute(t)
        rows = cur.fetchall()
elif tickers[ticker] == 'Акция привилегированная':
       t = f"""SELECT orderno, action, buysell, price, volume, time FROM PreferredStock
where TIME<= '%s' and seccode = '%s';""" % (time, ticker)
       cur.execute(t)
       rows = cur.fetchall()
elif 'облигац' in tickers[ticker].lower():
       t = f"""SELECT orderno, action, buysell, price, volume, time FROM Bonds
where TIME<= '%s' and seccode = '%s';""" % (time, ticker)
       cur.execute(t)
       rows = cur.fetchall()

# создаем списки для заявок и заявок-айсбергов
glass = [] #(buysell, price, volume)
icebergs = [] #(orderno, hidden_volume, time)

for elem in rows:
    # если action==1 (размещение заявки)
    if elem[1] == 1:
        # если объем не равен нулю
        if elem[4] != 0:
            # если список непустой
            if len(glass)>0:
                flag = False
                for j in glass:
                    # если в списке уже есть заявка такого же типа (buy/sell) с такой же ценой
                    if j[1] == elem[3] and j[0] == elem[2]:
                        # добавляем к исходному объему объем данной заявки
                        j[2] += elem[4]
                        flag = True
                        # выходим из цикла
                        break
                # в противном случае добавляем заявку в стакан
                if not flag:
                        glass.append([elem[2], elem[3], elem[4]])
            # если список пустой, добавляем заявку (первая заявка)
            else:   
                glass.append([elem[2], elem[3], elem[4]])  
        else:
            pass
    # если action=0 (снятие заявки)
    elif elem[1] == 0:
        # ищем в стакане нужную заявку
        for j in glass:
            # если тип заявки (buy/sell) и цена совпадают, значит мы нашли нужную заявку
            if j[1] == elem[3] and j[0] == elem[2]:
                # вычитаем соответствущий объем
                j[2] += -elem[4]
                # если оставшийся объем равен нулю, удаляем заявку
                if j[2] == 0:
                    del(j)
                # если объем стал отрицательным, это с большой долей вероятности означает, что мы имеем дело с заякой типа айсберг
                elif j[2] < 0:
                    # добавим заявку в соответствующий список
                    icebergs.append([elem[0], -j[2], elem[5]])
                    # удаляем заявку
                    del (j)
                # так как мы нашли нужную заявку, выходим из цикла
                break
    # если action=2 (сделка)
    elif elem[1] == 2:
        # ищем в стакане нужную заявку
        for j in glass:
            # если тип заявки (buy/sell) и цена совпадают, значит мы нашли нужную заявку
            if j[1] == elem[3] and j[0] == elem[2]:
                # вычитаем соответствущий объем
                j[2] += -elem[4]
                # если оставшийся объем равен нулю, удаляем заявку
                if j[2] == 0:
                    del(j)
                # если объем стал отрицательным, это с большой долей вероятности означает, что мы имеем дело с заякой типа айсберг
                elif j[2] < 0:
                    # добавим заявку в соответствующий список
                    icebergs.append([elem[0], -j[2], elem[5]])
                    # удаляем заявку
                    del (j)
                break

# На всякий случай еще раз "очистим" стакан от заявок с нулевым объемом
glass_new = []
for j in range(0,len(glass)):
    if glass[j][2]!=0:
        glass_new.append(glass[j])


# Из списка glass_new создадим датафрейм
df = pd.DataFrame.from_records(glass_new, columns=['buy/sell', 'price', 'volume'])
# создадим отдельные колонки для объема ask и bid (ноль в i-ой строке означает, что по данной цене была отправлена заявка противоположного типа)
df['buy_volume'] = np.where(df['buy/sell']=='B', df['volume'], 0)
df['sell_volume'] = np.where(df['buy/sell']=='S', df['volume'], 0)
del df['buy/sell']
del df['volume']
df.sort_values('price', inplace=True, ascending=False)
df = df.reindex(columns=['buy_volume', 'price', 'sell_volume'])
# таблица, визуализирующая стакан
print(df)

# оставляем только ненулевые значения
buy = df.loc[(df['buy_volume']>0)]
sell = df.loc[(df['sell_volume']>0)]

# Создаем  соответствующие списки
bid_price = buy['price']
bid_volume = buy['buy_volume']
ask_price = sell['price']
ask_volume = sell['sell_volume']


# Строим график
fig, ax = plt.subplots()
plt.plot(ask_price, ask_volume, color='red', label='asks', marker='o', markersize=2)
plt.text(ask_price[-1:], ask_volume[-1:], str(int(ask_volume[-1:])))
plt.plot(bid_price, bid_volume, color='green', label='bids', marker='o', markersize=2)
plt.text(bid_price[:1], bid_volume[:1], str(int(bid_volume[:1])))
plt.text(bid_price[-1:], max(max(ask_volume), max(bid_volume)), 'bid-ask spread = %s' % str(round(float(ask_price[-1:])-float(bid_price[:1]), 3)))
ax.set(xlabel='Price', ylabel='volume')
plt.legend()
plt.show()

# Таблица заявок типа айсберг
iceberg = pd.DataFrame.from_records(icebergs, columns=['orderno', 'hidden_volume', 'time'])
print(iceberg)