"""Name and phone cleaning tests."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from sales_analytics.transformation import clean_customer_names, clean_customer_phones

_CUST_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
])


class TestCleanCustomerNames:
    """Covers digits, leetspeak, special chars, whitespace, and title case."""

    # (id, dirty_input, expected_clean_output)
    DIRTY_NAME_CASES = [
        # Embedded multi-digit removal
        (1, "Gary567 Hansen", "Gary Hansen"),
        (2, "Mitch Willin0009gham", "Mitch Willingham"),
        (3, "   _Mike Vitt 12313orini", "Mike Vittorini"),
        (4, "Brosin34a Hoffman 09", "Brosina Hoffman"),
        (5, "Victo 3456ria Brennan", "Victoria Brennan"),
        (6, "Ben Pet 987 -=erman", "Ben Peterman"),
        (7, "Tracy P 908765oddar", "Tracy Poddar"),
        (8, "Craig M 876olinari", "Craig Molinari"),
        (9, "5467Ben Wallace", "Ben Wallace"),
        (10, "Anth 34789ony Witt", "Anthony Witt"),
        (11, "Ge 1234orge Bell", "George Bell"),
        (12, "     Rac5467hel Payne", "Rachel Payne"),
        (13, "6789Erin  Mull", "Erin Mull"),
        (14, "654Dean Braden", "Dean Braden"),
        (15, "Fra9876nk Gasti  ;.,.,neau", "Frank Gastineau"),
        (16, "Beth Tho098-.,;;mpson", "Beth Thompson"),
        (17, "_12312Patrick Bzostek", "Patrick Bzostek"),
        # Leetspeak substitutions (1l->ll, 11->ll, 55->ss)
        (18, "Bi1l Stewart", "Bill Stewart"),
        (19, "Bi1l Overfelt", "Bill Overfelt"),
        (20, "Bi1l Eplett", "Bill Eplett"),
        (21, "Bi1l Donatelli", "Bill Donatelli"),
        (22, "Bi1l Tyler", "Bill Tyler"),
        (23, "Ji11 Stevenson", "Jill Stevenson"),
        (24, "Helen Wa55erman", "Helen Wasserman"),
        (25, "Fred Wa55erman", "Fred Wasserman"),
        (26, "Joni Wa55erman", "Joni Wasserman"),
        # Single-char substitutions (0->o, @->a, !->i)
        (27, "N0ra Paige", "Nora Paige"),
        (28, "C@thy Armstrong", "Cathy Armstrong"),
        (29, "Karen Dan!els", "Karen Daniels"),
        (30, "Maribeth 5chnelling", "Maribeth Schnelling"),
        # Special character removal
        (31, "Shirl)(*&ey Schmidt", "Shirley Schmidt"),
        (32, "Neil Knudson%^&*(", "Neil Knudson"),
        (33, ")(*&Sung Pak", "Sung Pak"),
        (34, "&^*(5678Shirley Daniels", "Shirley Daniels"),
        (35, "B*#^%ruce Geld", "Bruce Geld"),
        (36, "Rich.-=%&*ard Eichhorn", "Richard Eichhorn"),
        (37, "Cynthia A;,.[]rntzen", "Cynthia Arntzen"),
        (38, "Muhammed YeDwab#", "Muhammed Yedwab"),
        (39, "John Hus<>><>ton", "John Huston"),
        (40, "Sally Knut_son", "Sally Knutson"),
        (41, "Paul Knut_son", "Paul Knutson"),
        (42, "Dean perceR__#$#", "Dean Percer"),
        (43, "\":[]{}-=Charles Crestani", "Charles Crestani"),
        # Whitespace and trim
        (44, " &&Tracy Blumstein", "Tracy Blumstein"),
        (45, "             Dorothy Wardle", "Dorothy Wardle"),
        (46, " Elpida Rittenbach ", "Elpida Rittenbach"),
        (47, "         =--Katharine Harms", "Katharine Harms"),
        (48, "         Helen Abelman", "Helen Abelman"),
        # Multi-space fragment merging
        (49, "Ad.       ..am Hart", "Adam Hart"),
        (50, "B         ecky Martin", "Becky Martin"),
        (51, "Tho   12 =-.mas Boland", "Thomas Boland"),
        (52, " Shahi  Hopkins", "Shahi Hopkins"),
        (53, " Shahi  Shariari", "Shahi Shariari"),
        (54, "Kat rina Bavinger", "Katrina Bavinger"),
        (55, "Kat rina Edelman", "Katrina Edelman"),
        # Compound patterns (special chars + digits + spaces)
        (56, "Tam&^*ara Willing___)ham", "Tamara Willingham"),
        (57, "Pete@#$ Takahito", "Petea Takahito"),
        (58, "       Kristi;'[]na Nunn", "Kristi'Na Nunn"),
        # Trailing punctuation removal
        (59, "Jason Fortune-", "Jason Fortune"),
        (60, "Joy Bell-", "Joy Bell"),
        (61, "[]-=;''Becky Pak", "Becky Pak"),
        # Title case normalization
        (62, "Dorris liebe", "Dorris Liebe"),
        # Apostrophe preservation
        (63, "Mary O'Rourke", "Mary O'Rourke"),
    ]

    def test_cleans_all_dirty_name_patterns(self, spark: SparkSession) -> None:

        data = [(id_, dirty) for id_, dirty, _ in self.DIRTY_NAME_CASES]
        df = spark.createDataFrame(data, _CUST_SCHEMA)


        results = {
            r["id"]: r["customer_name"]
            for r in clean_customer_names(df).collect()
        }


        for id_, dirty, expected in self.DIRTY_NAME_CASES:
            assert results[id_] == expected, (
                f"ID {id_}: '{dirty}' -> expected '{expected}', got '{results[id_]}'"
            )

    def test_null_passthrough(self, spark: SparkSession) -> None:
        schema = StructType([StructField("customer_name", StringType(), True)])
        df = spark.createDataFrame([(None,)], schema)
        assert clean_customer_names(df).collect()[0]["customer_name"] is None


class TestCleanCustomerPhones:
    """Covers dotted, dashed, country-code, extension, and invalid formats."""

    def test_formats_and_rejects_phones(self, spark: SparkSession) -> None:

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("phone", StringType(), True),
        ])
        data = [
            (1, "421.580.0902x9815"),
            (2, "001-542-415-0246x314"),
            (3, "7185624866"),
            (4, "770-493-4211"),
            (5, "576.093.6933x5404"),
            (6, "265.101.5569x1098"),
            (7, "(563)647-4830x5318"),
            (8, "#ERROR!"),
            (9, "-6181"),
            (10, None),
        ]
        df = spark.createDataFrame(data, schema)


        results = {
            r["id"]: r["phone"]
            for r in clean_customer_phones(df).collect()
        }

        # valid phones
        assert results[1] == "(421) 580-0902 x9815"
        assert results[2] == "(542) 415-0246 x314"
        assert results[3] == "(718) 562-4866"
        assert results[4] == "(770) 493-4211"
        assert results[5] == "(576) 093-6933 x5404"
        assert results[6] == "(265) 101-5569 x1098"
        assert results[7] == "(563) 647-4830 x5318"

        # invalid phones
        assert results[8] is None
        assert results[9] is None
        assert results[10] is None
