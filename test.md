``` 
SELECT upper('é' collate "C") AS "C", upper('é' collate "C.utf8") AS "C.utf8", upper('é' collate "ucs_basic") AS "ucs_basic";
```
| C | C.utf8 | ucs\_basic |
|:--|:-------|:----------|
| é | É | é |
> ``` status
> SELECT 1
> ```

``` 
SELECT 'A' AS "0041", E'\u0378' AS "0378", ('A' < E'\u0378' collate "C") AS "C", ('A' < E'\u0378' collate "C.utf8") AS "C.utf8", ('A' < E'\u0378' collate "ucs_basic") AS "ucs_basic", ('A' < E'\u0378' collate "en_GB") AS "en_GB";
```
| 0041 | 0378 | C | C.utf8 | ucs\_basic | en\_GB |
|:-----|:-----|:--|:-------|:----------|:------|
| A | ͸ | t | f | t | f |
> ``` status
> SELECT 1
> ```

``` 
SELECT 'A' AS "0041", E'\u0377' AS "0377", ('A' < E'\u0377' collate "C") AS "C", ('A' < E'\u0377' collate "C.utf8") AS "C.utf8", ('A' < E'\u0377' collate "ucs_basic") AS "ucs_basic", ('A' < E'\u0377' collate "en_GB") AS "en_GB";
```
| 0041 | 0377 | C | C.utf8 | ucs\_basic | en\_GB |
|:-----|:-----|:--|:-------|:----------|:------|
| A | ͷ | t | t | t | t |
> ``` status
> SELECT 1
> ```

``` 
SELECT E'\u00E9' AS "00E9", E'\u00D3' AS "00D3", (E'\u00E9' < E'\u00D3' collate "C") AS "C", (E'\u00E9' < E'\u00D3' collate "C.utf8") AS "C.UTF-8", (E'\u00E9' < E'\u00D3' collate "ucs_basic") AS "ucs_basic", (E'\u00E9' < E'\u00D3' collate "en_GB") AS "en_GB";
```
| 00E9 | 00D3 | C | C.UTF-8 | ucs\_basic | en\_GB |
|:-----|:-----|:--|:--------|:----------|:------|
| é | Ó | f | f | f | t |
> ``` status
> SELECT 1
> ```

``` 
SELECT E'\u0510' AS "0510", E'\u08BA' AS "08BA", (E'\u0510' < E'\u08BA' collate "C") AS "C", (E'\u0510' < E'\u08BA' collate "C.utf8") AS "C.utf8", (E'\u0510' < E'\u08BA' collate "ucs_basic") AS "ucs_basic", (E'\u0510' < E'\u08BA' collate "en_GB") AS "en_GB";
```
| 0510 | 08BA | C | C.utf8 | ucs\_basic | en\_GB |
|:-----|:-----|:--|:-------|:----------|:------|
| Ԑ | ࢺ | t | t | t | t |
> ``` status
> SELECT 1
> ```

``` 
SELECT ('ab' < 'a-c' collate "C") AS "C", ('ab' < 'a-c' collate "C.utf8") AS "C.utf8", ('ab' < 'a-c' collate "ucs_basic") AS "ucs_basic", ('ab' < 'a-c' collate "en_GB") AS "en_GB";

```
| C | C.utf8 | ucs\_basic | en\_GB |
|:--|:-------|:----------|:------|
| f | f | f | t |
> ``` status
> SELECT 1
> ```

``` 
select * from pg_collation where collprovider='c' order by "collname";
```
| oid | collname | collnamespace | collowner | collprovider | collisdeterministic | collencoding | collcollate | collctype | colliculocale | collversion |
|:----|:---------|:--------------|:----------|:-------------|:--------------------|-------------:|:------------|:----------|:--------------|:------------|
| 950 | C | 11 | 10 | c | t | -1 | C | C | *null* | *null* |
| 12341 | C.utf8 | 11 | 10 | c | t | 6 | C.utf8 | C.utf8 | *null* | *null* |
| 951 | POSIX | 11 | 10 | c | t | -1 | POSIX | POSIX | *null* | *null* |
| 12342 | en\_AG | 11 | 10 | c | t | 6 | en\_AG | en\_AG | *null* | 2.28 |
| 12378 | en\_AU | 11 | 10 | c | t | 6 | en\_AU.utf8 | en\_AU.utf8 | *null* | 2.28 |
| 12343 | en\_AU | 11 | 10 | c | t | 8 | en\_AU | en\_AU | *null* | 2.28 |
| 12344 | en\_AU.utf8 | 11 | 10 | c | t | 6 | en\_AU.utf8 | en\_AU.utf8 | *null* | 2.28 |
| 12345 | en\_BW | 11 | 10 | c | t | 8 | en\_BW | en\_BW | *null* | 2.28 |
| 12379 | en\_BW | 11 | 10 | c | t | 6 | en\_BW.utf8 | en\_BW.utf8 | *null* | 2.28 |
| 12346 | en\_BW.utf8 | 11 | 10 | c | t | 6 | en\_BW.utf8 | en\_BW.utf8 | *null* | 2.28 |
| 12380 | en\_CA | 11 | 10 | c | t | 6 | en\_CA.utf8 | en\_CA.utf8 | *null* | 2.28 |
| 12347 | en\_CA | 11 | 10 | c | t | 8 | en\_CA | en\_CA | *null* | 2.28 |
| 12348 | en\_CA.utf8 | 11 | 10 | c | t | 6 | en\_CA.utf8 | en\_CA.utf8 | *null* | 2.28 |
| 12349 | en\_DK | 11 | 10 | c | t | 8 | en\_DK | en\_DK | *null* | 2.28 |
| 12381 | en\_DK | 11 | 10 | c | t | 6 | en\_DK.utf8 | en\_DK.utf8 | *null* | 2.28 |
| 12350 | en\_DK.utf8 | 11 | 10 | c | t | 6 | en\_DK.utf8 | en\_DK.utf8 | *null* | 2.28 |
| 12351 | en\_GB | 11 | 10 | c | t | 8 | en\_GB | en\_GB | *null* | 2.28 |
| 12382 | en\_GB | 11 | 10 | c | t | 16 | en\_GB.iso885915 | en\_GB.iso885915 | *null* | 2.28 |
| 12383 | en\_GB | 11 | 10 | c | t | 6 | en\_GB.utf8 | en\_GB.utf8 | *null* | 2.28 |
| 12352 | en\_GB.iso885915 | 11 | 10 | c | t | 16 | en\_GB.iso885915 | en\_GB.iso885915 | *null* | 2.28 |
| 12353 | en\_GB.utf8 | 11 | 10 | c | t | 6 | en\_GB.utf8 | en\_GB.utf8 | *null* | 2.28 |
| 12354 | en\_HK | 11 | 10 | c | t | 8 | en\_HK | en\_HK | *null* | 2.28 |
| 12384 | en\_HK | 11 | 10 | c | t | 6 | en\_HK.utf8 | en\_HK.utf8 | *null* | 2.28 |
| 12355 | en\_HK.utf8 | 11 | 10 | c | t | 6 | en\_HK.utf8 | en\_HK.utf8 | *null* | 2.28 |
| 12356 | en\_IE | 11 | 10 | c | t | 8 | en\_IE | en\_IE | *null* | 2.28 |
| 12385 | en\_IE | 11 | 10 | c | t | 6 | en\_IE.utf8 | en\_IE.utf8 | *null* | 2.28 |
| 12358 | en\_IE.utf8 | 11 | 10 | c | t | 6 | en\_IE.utf8 | en\_IE.utf8 | *null* | 2.28 |
| 12357 | en\_IE@euro | 11 | 10 | c | t | 16 | en\_IE@euro | en\_IE@euro | *null* | 2.28 |
| 12359 | en\_IL | 11 | 10 | c | t | 6 | en\_IL | en\_IL | *null* | 2.28 |
| 12360 | en\_IN | 11 | 10 | c | t | 6 | en\_IN | en\_IN | *null* | 2.28 |
| 12361 | en\_NG | 11 | 10 | c | t | 6 | en\_NG | en\_NG | *null* | 2.28 |
| 12386 | en\_NZ | 11 | 10 | c | t | 6 | en\_NZ.utf8 | en\_NZ.utf8 | *null* | 2.28 |
| 12362 | en\_NZ | 11 | 10 | c | t | 8 | en\_NZ | en\_NZ | *null* | 2.28 |
| 12363 | en\_NZ.utf8 | 11 | 10 | c | t | 6 | en\_NZ.utf8 | en\_NZ.utf8 | *null* | 2.28 |
| 12387 | en\_PH | 11 | 10 | c | t | 6 | en\_PH.utf8 | en\_PH.utf8 | *null* | 2.28 |
| 12364 | en\_PH | 11 | 10 | c | t | 8 | en\_PH | en\_PH | *null* | 2.28 |
| 12365 | en\_PH.utf8 | 11 | 10 | c | t | 6 | en\_PH.utf8 | en\_PH.utf8 | *null* | 2.28 |
| 12388 | en\_SC | 11 | 10 | c | t | 6 | en\_SC.utf8 | en\_SC.utf8 | *null* | 2.28 |
| 12366 | en\_SC.utf8 | 11 | 10 | c | t | 6 | en\_SC.utf8 | en\_SC.utf8 | *null* | 2.28 |
| 12389 | en\_SG | 11 | 10 | c | t | 6 | en\_SG.utf8 | en\_SG.utf8 | *null* | 2.28 |
| 12367 | en\_SG | 11 | 10 | c | t | 8 | en\_SG | en\_SG | *null* | 2.28 |
| 12368 | en\_SG.utf8 | 11 | 10 | c | t | 6 | en\_SG.utf8 | en\_SG.utf8 | *null* | 2.28 |
| 12390 | en\_US | 11 | 10 | c | t | 16 | en\_US.iso885915 | en\_US.iso885915 | *null* | 2.28 |
| 12369 | en\_US | 11 | 10 | c | t | 8 | en\_US | en\_US | *null* | 2.28 |
| 12391 | en\_US | 11 | 10 | c | t | 6 | en\_US.utf8 | en\_US.utf8 | *null* | 2.28 |
| 12371 | en\_US.iso885915 | 11 | 10 | c | t | 16 | en\_US.iso885915 | en\_US.iso885915 | *null* | 2.28 |
| 12372 | en\_US.utf8 | 11 | 10 | c | t | 6 | en\_US.utf8 | en\_US.utf8 | *null* | 2.28 |
| 12392 | en\_US@ampm | 11 | 10 | c | t | 6 | en\_US@ampm.UTF-8 | en\_US@ampm.UTF-8 | *null* | 2.28 |
| 12370 | en\_US@ampm.UTF-8 | 11 | 10 | c | t | 6 | en\_US@ampm.UTF-8 | en\_US@ampm.UTF-8 | *null* | 2.28 |
| 12373 | en\_ZA | 11 | 10 | c | t | 8 | en\_ZA | en\_ZA | *null* | 2.28 |
| 12393 | en\_ZA | 11 | 10 | c | t | 6 | en\_ZA.utf8 | en\_ZA.utf8 | *null* | 2.28 |
| 12374 | en\_ZA.utf8 | 11 | 10 | c | t | 6 | en\_ZA.utf8 | en\_ZA.utf8 | *null* | 2.28 |
| 12375 | en\_ZM | 11 | 10 | c | t | 6 | en\_ZM | en\_ZM | *null* | 2.28 |
| 12376 | en\_ZW | 11 | 10 | c | t | 8 | en\_ZW | en\_ZW | *null* | 2.28 |
| 12394 | en\_ZW | 11 | 10 | c | t | 6 | en\_ZW.utf8 | en\_ZW.utf8 | *null* | 2.28 |
| 12377 | en\_ZW.utf8 | 11 | 10 | c | t | 6 | en\_ZW.utf8 | en\_ZW.utf8 | *null* | 2.28 |
| 12340 | ucs\_basic | 11 | 10 | c | t | 6 | C | C | *null* | *null* |
> ``` status
> SELECT 57
> ```

[fiddle](https://dbfiddle.uk/xs6-Di0k)
