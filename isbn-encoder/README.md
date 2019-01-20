# isbn-encoder

The function should check if the content in column _isbn_ is a [valid 13-digit isbn code](https://en.wikipedia.org/wiki/International_Standard_Book_Number) and create new rows for each part of the ISBN code.

#### Describe your solution

__TODO:__ Please explain your solution briefly and highlight the advantages and disadvantages of your implementation.

### Example

#### Input

| Name        | Year           | ISBN  |
| ----------- |:--------------:|-------|
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN: 978-1449358624 |

#### Output

| Name        | Year           | ISBN  |
| ----------- |:--------------:|-------|
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN: 978-1449358624 |
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN-EAN: 978 |
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN-GROUP: 14 |
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN-PUBLISHER: 4935 |
| Learning Spark: Lightning-Fast Big Data Analysis      | 2015 | ISBN-TITLE: 862 


### Solution
Create class called RuleItem, regulate Isbn's GroupStr headCode, GroupStr length, Publisher headCode, Publisher length, BookTitle headCode, BookTitle length.(Not consider EAN code, because all EAN code start with 978, but Group code, Publisher code and Title code are mutative)

```sh
case class RuleItem(groupStr: String, groupLen: Int, publisherStr: String, publisherLen: Int, titleStr: String, titleLen: Int)
```

Add all RuleItem to ruleArray.
```sh
val ruleArray = Array(ruleItem1)
```

Loop ruleArray, if the input Isbn match all requriments in one of ruleItem, explode it to 5 rows(including itself, EAN,Group, publisher and Title).
If match none of ruleItems, output itself.

Advantage:
Customer could customize requirements of Isbn components' headCode and their length. 
Rules could be written into csv form, which would preform better readability.

Disadvantage:
RuleItem has just been save by Array temporarily， not by TreeSet, there would be some redudant information.








