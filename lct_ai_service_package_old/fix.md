1) Нельзя просто так расшарить пути для файлов между воркерами, но я могу загружать файл в hadoop, необходимо что бы аишка генерировала даг с учетом ссылки на файл в hadoop и оттуда забиралось бы

2) немного изменился запрос, пример реального запроса:

```json
{
  "request_uid" : "21c67120-ea9c-4b95-b441-4af19dc418c2",
  "pipeline_uid" : "01999ff0-dc7a-7465-9db8-ccd71d27e3a7",
  "timestamp" : "2025-10-01T13:23:57.2155768Z",
  "sources" : [ {
    "$type" : "csv",
    "parameters" : {
      "delimiter" : ";",
      "type" : "csv",
      "file_path" : "C:\\Projects\\datapipeline\\src\\Server\\Services\\DataProcessing\\API\\files\\01999ff0-dc7e-759f-ab54-d59ca2d07bf1\\part-00000-37dced01-2ad2-48c8-a56d-54b4d8760599-c000.csv"
    },
    "schema_infos" : [ {
      "column_count" : 27,
      "columns" : [ {
        "name" : "created",
        "path" : "created",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2021-08-18T16:01:14.583+03:00", "2021-09-16T18:45:54.401+03:00", "2021-05-06T12:02:04.808+03:00", "2021-05-25T16:05:06.883+03:00", "2021-05-07T14:49:50.473+03:00", "2021-09-09T18:44:52.759+03:00", "2021-04-19T12:58:13.280+03:00", "2021-07-25T13:36:43.379+03:00", "2020-10-21T10:27:45.651+03:00", "2021-06-01T16:06:28.613+03:00" ],
        "unique_values" : 428460,
        "null_count" : 0,
        "statistics" : {
          "min" : "29",
          "max" : "181",
          "avg" : "29.01"
        }
      }, {
        "name" : "order_status",
        "path" : "order_status",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "PAID", "REFUND", "CANCELLED", "NEW" ],
        "unique_values" : 4,
        "null_count" : 52,
        "statistics" : {
          "min" : "3",
          "max" : "9",
          "avg" : "4.00"
        }
      }, {
        "name" : "ticket_status",
        "path" : "ticket_status",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "PAID", "REFUND", "CANCELLED", "NEW" ],
        "unique_values" : 4,
        "null_count" : 52,
        "statistics" : {
          "min" : "3",
          "max" : "9",
          "avg" : "4.00"
        }
      }, {
        "name" : "ticket_price",
        "path" : "ticket_price",
        "data_type" : "float",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0.0", "250.0", "800.0", "400.0", "50.0", "150.0", "600.0", "1000.0", "200.0", "300.0" ],
        "unique_values" : 95,
        "null_count" : 166,
        "statistics" : {
          "min" : "0.0",
          "max" : "19500.0",
          "avg" : "273.63"
        }
      }, {
        "name" : "visitor_category",
        "path" : "visitor_category",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Обучающиеся по очной форме обучения в государственных образовательных учреждениях и негосударственных образовательных организациях, имеющих государственную аккредитацию, по программам начального общего, основного общего, среднего (полного) общего образования", "Взрослые граждане РФ и СНГ", "Студенты, обучающиеся по очной форме обучения в государственных образовательных учреждениях и негосударственных образовательных организациях, имеющих государственную аккредитацию, по программам среднего и высшего профессионального образования", "Дети до 7 лет (не достигшие семилетнего возраста)", "Иностранные граждане", "Дети в возрасте от 7 до 18 лет", "Дети в возрасте от 7 до 17 лет включительно", "Обучающиеся по очной форме обучения в образовательных учреждениях по программам начального профессионального образования", "Пенсионеры", "Лица из многодетных семей – семьи с тремя и более детьми до достижения младшим ребенком возраста 16 лет (учащегося в образовательных учреждениях, реализующих общеобразовательные программы – до 18 лет)" ],
        "unique_values" : 82,
        "null_count" : 166,
        "statistics" : {
          "min" : "5",
          "max" : "552",
          "avg" : "52.99"
        }
      }, {
        "name" : "event_id",
        "path" : "event_id",
        "data_type" : "integer",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "7561", "1765", "1473", "656", "4708", "1963", "2954", "1093", "2263", "3817" ],
        "unique_values" : 7822,
        "null_count" : 166,
        "statistics" : {
          "min" : "171",
          "max" : "20042",
          "avg" : "4028.49"
        }
      }, {
        "name" : "is_active",
        "path" : "is_active",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "true" ],
        "unique_values" : 1,
        "null_count" : 52,
        "statistics" : {
          "min" : "4",
          "max" : "4",
          "avg" : "4.00"
        }
      }, {
        "name" : "valid_to",
        "path" : "valid_to",
        "data_type" : "date",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2021-08-18", "2021-09-18", "2021-05-06", "2021-05-25", "2021-05-07", "2021-09-19", "2021-04-25", "2021-07-25", "2020-10-21", "2021-06-01" ],
        "unique_values" : 894,
        "null_count" : 52,
        "statistics" : {
          "min" : "2020-08-21",
          "max" : "2023-06-13",
          "avg" : ""
        }
      }, {
        "name" : "count_visitor",
        "path" : "count_visitor",
        "data_type" : "integer",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1" ],
        "unique_values" : 1,
        "null_count" : 52,
        "statistics" : {
          "min" : "1",
          "max" : "1",
          "avg" : "1.00"
        }
      }, {
        "name" : "is_entrance",
        "path" : "is_entrance",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "true", "false" ],
        "unique_values" : 2,
        "null_count" : 95,
        "statistics" : {
          "min" : "4",
          "max" : "5",
          "avg" : "4.34"
        }
      }, {
        "name" : "is_entrance_mdate",
        "path" : "is_entrance_mdate",
        "data_type" : "date",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2021-08-18T19:14:45.427+03:00", "2021-09-16T18:45:54.977+03:00", "2021-05-06T12:03:08.989+03:00", "2021-05-25T17:15:54.616+03:00", "2021-05-07T14:49:50.822+03:00", "2021-09-09T18:44:54.363+03:00", "2021-04-19T12:58:14.791+03:00", "2021-07-25T13:39:15.750+03:00", "2020-10-21T10:27:46.139+03:00", "2021-06-01T16:06:29.011+03:00" ],
        "unique_values" : 478317,
        "null_count" : 95,
        "statistics" : {
          "min" : "2020-08-21",
          "max" : "2023-01-10",
          "avg" : ""
        }
      }, {
        "name" : "event_name",
        "path" : "event_name",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Бальный танец", "Посещение Дворца (Большого дома)", "Посещение Московского зоопарка", "Посещение постоянных экспозиций и временных выставок Большого дворца", "Театрализованная экскурсия в Измайловском парке", "Парк музея-усадьбы \"Кусково\"", "Посещение экспозиции \"Величие и глубина\" (без экскурсии)", "Основная экспозиция \"Галерея Ильи Глазунова\"", "Test", "Детский спектакль" ],
        "unique_values" : 7303,
        "null_count" : 166,
        "statistics" : {
          "min" : "1",
          "max" : "200",
          "avg" : "36.51"
        }
      }, {
        "name" : "event_kind_name",
        "path" : "event_kind_name",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "консультация, методическое занятие, семинар, тренинг", "выставка", "экскурсия", "представление, спектакль, инсценировка, перфоманс, театрализованное представление", "интерактивное занятие, мастер-класс, урок", "викторина", "концерт, музыкальное представление", "презентация, демонстрация, показетльные выступления", "день открытых дверей", "квест" ],
        "unique_values" : 33,
        "null_count" : 166,
        "statistics" : {
          "min" : "5",
          "max" : "114",
          "avg" : "12.36"
        }
      }, {
        "name" : "spot_id",
        "path" : "spot_id",
        "data_type" : "integer",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "274010", "69", "276345", "156", "276214", "276181", "86", "1747", "9", "179" ],
        "unique_values" : 359,
        "null_count" : 166,
        "statistics" : {
          "min" : "1",
          "max" : "279480",
          "avg" : "206019.49"
        }
      }, {
        "name" : "spot_name",
        "path" : "spot_name",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Шверника ул. 13, корпус 2", "Усадьба Кусково", "Грузинская Б. ул. 1, строение 1", "Дольская ул. 1, строение 6", "Большого Круга аллея 7", "Свободы ул. 52", "Галерея Ильи Глазунова", "Тестовая площадка для экскурсий", "Самотечный 1-й пер. 9, строение 1", "Дольская ул. 1, строение 5" ],
        "unique_values" : 367,
        "null_count" : 166,
        "statistics" : {
          "min" : "6",
          "max" : "95",
          "avg" : "28.39"
        }
      }, {
        "name" : "museum_name",
        "path" : "museum_name",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Государственное бюджетное учреждение культуры города Москвы «Центр культуры и досуга «Академический»", "Государственное бюджетное учреждение культуры города Москвы «Музей-усадьба «Кусково»", "Государственное автономное учреждение города Москвы «Московский государственный зоологический парк»", "Государственное бюджетное учреждение культуры города Москвы «Государственный историко-архитектурный, художественный и ландшафтный музей-заповедник «Царицыно»", "Государственное автономное учреждение культуры города Москвы «Измайловский Парк культуры и отдыха»", "Государственное автономное учреждение культуры города Москвы «Музейно-парковый комплекс «Северное Тушино»", "Государственное бюджетное учреждение культуры города Москвы «Московская государственная картинная галерея народного художника СССР Ильи Глазунова»", "Тестовый музей  для виджета", "Государственное бюджетное учреждение культуры города Москвы «Государственный музей истории ГУЛАГа»", "Государственное автономное учреждение культуры города Москвы «Усадьба Воронцово»" ],
        "unique_values" : 142,
        "null_count" : 166,
        "statistics" : {
          "min" : "27",
          "max" : "179",
          "avg" : "102.55"
        }
      }, {
        "name" : "start_datetime",
        "path" : "start_datetime",
        "data_type" : "date",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2021-08-18 17:00:00", "2021-09-18 10:00:00", "2021-05-06 07:30:00", "2021-05-25 07:30:00", "2021-05-07 07:30:00", "2021-04-25 14:00:00", "2021-07-25 10:00:00", "2020-10-21 09:00:00", "2021-06-01 18:00:00", "2021-04-24 11:00:00" ],
        "unique_values" : 10946,
        "null_count" : 19848,
        "statistics" : {
          "min" : "2020-05-07",
          "max" : "2023-06-13",
          "avg" : ""
        }
      }, {
        "name" : "ticket_id",
        "path" : "ticket_id",
        "data_type" : "integer",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1778482", "2100707", "881269", "1147456", "907812", "2004598", "704383", "1534827", "46042", "1216042" ],
        "unique_values" : 478232,
        "null_count" : 78,
        "statistics" : {
          "min" : "30386",
          "max" : "7934236",
          "avg" : "3574708.41"
        }
      }, {
        "name" : "update_timestamp",
        "path" : "update_timestamp",
        "data_type" : "date",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2021-08-18T16:01:15.682+03:00", "2021-09-16T18:46:19.713+03:00", "2021-05-06T12:02:05.566+03:00", "2021-05-25T16:05:07.733+03:00", "2021-05-07T14:50:59.624+03:00", "2021-09-09T18:48:33.380+03:00", "2021-04-19T12:58:18.983+03:00", "2021-07-25T13:38:03.993+03:00", "2020-10-21T11:30:22.535+03:00", "2021-06-01T16:06:29.722+03:00" ],
        "unique_values" : 428458,
        "null_count" : 78,
        "statistics" : {
          "min" : "2020-08-21",
          "max" : "2023-01-10",
          "avg" : ""
        }
      }, {
        "name" : "client_name",
        "path" : "client_name",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "ШУКУРОВ РУСЛАН", "КОБЕЛЬКОВА АЛЁНА", "БОБРОВ ЮРИЙ", "КРЕБСА СОФИЯ", "ПЕТРОСЯНА ГАЛИНА", "ГРЖИБОВСКИЙ ВИКТОР", "ЧИХУТИН ИЛЬЯ", "ШУМ ДЕНИС", "АБДУЛЛАЕВА АННА", "МАЛАНЧУКА МАРИНА" ],
        "unique_values" : 335332,
        "null_count" : 211,
        "statistics" : {
          "min" : "4",
          "max" : "47",
          "avg" : "15.21"
        }
      }, {
        "name" : "name",
        "path" : "name",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "КИРИЛЛ", "СЕРГЕЙ", "ИЛЬЯ", "ДМИТРИЙ", "АЛСУ", "САМАГАН", "ЮРИЙ", "ОЛЬГА", "АЛЕКСАНДР", "ДАНИИЛ" ],
        "unique_values" : 3265,
        "null_count" : 192489,
        "statistics" : {
          "min" : "1",
          "max" : "32",
          "avg" : "6.37"
        }
      }, {
        "name" : "surname",
        "path" : "surname",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "ШУКУРОВ", "КОБЕЛЬКОВ", "КРЕБС", "ПЕТРОСЯН", "ЧИХУТИНА", "ШУМ", "АБДУЛЛАЕВ", "МАЛАНЧУКА", "КАБАНОВ", "КОЗЛОВ" ],
        "unique_values" : 13529,
        "null_count" : 192489,
        "statistics" : {
          "min" : "2",
          "max" : "33",
          "avg" : "7.84"
        }
      }, {
        "name" : "client_phone",
        "path" : "client_phone",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "79859482165", "79264765873", "79116487504", "79776503726", "79528133791", "79296127031", "79963849137", "79689627566", "79026704646", "79162039150" ],
        "unique_values" : 321224,
        "null_count" : 157064,
        "statistics" : {
          "min" : "10",
          "max" : "1298",
          "avg" : "11.47"
        }
      }, {
        "name" : "museum_inn",
        "path" : "museum_inn",
        "data_type" : "integer",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "3832203597", "0415706141", "8719976264", "1678121766", "2599957179", "5156237758", "6377638424", "8725097083", "1119402769", "9645873908" ],
        "unique_values" : 478405,
        "null_count" : 192,
        "statistics" : {
          "min" : "55758",
          "max" : "9999982413",
          "avg" : "5000139801.15"
        }
      }, {
        "name" : "birthday_date",
        "path" : "birthday_date",
        "data_type" : "date",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1989-10-28", "1977-12-30", "2015-08-17", "1984-04-22", "1989-12-29", "1989-12-12", "1956-05-04", "1985-08-24", "2014-06-28", "2019-12-19" ],
        "unique_values" : 16358,
        "null_count" : 447307,
        "statistics" : {
          "min" : "1862-01-16",
          "max" : "2022-02-17",
          "avg" : ""
        }
      }, {
        "name" : "order_number",
        "path" : "order_number",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "75343-483088", "04091-683930", "87693-505050", "36421-684592", "72297-435006", "13255-946527", "03160-598428", "31416-518350", "25662-034092", "49181-880487" ],
        "unique_values" : 478536,
        "null_count" : 78,
        "statistics" : {
          "min" : "12",
          "max" : "12",
          "avg" : "12.00"
        }
      }, {
        "name" : "ticket_number",
        "path" : "ticket_number",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "07a16922-969c-1033-a23f-f20b57dcf045", "2d6fcdd1-c16d-9501-bda8-ae329e98a5ca", "06c34a5c-b6b7-1b92-b245-460c1a7ce3e9", "22f9764a-a036-2fb8-ac3a-2c98a58784b9", "441c1a65-1d3a-3e8a-85a4-be47e023f563", "476449d7-532a-83d0-a447-e7d7bca2bce9", "9fabeaed-85b7-7856-206d-66ce893c6aa7", "6f1125ac-38ad-02b1-a548-5f9e178e58dd", "3908c213-347f-7bd3-1af0-0f625b2cba63", "57e540bd-b099-9c71-a6ed-e143eca6b5ec" ],
        "unique_values" : 478423,
        "null_count" : 192,
        "statistics" : {
          "min" : "36",
          "max" : "36",
          "avg" : "36.00"
        }
      } ],
      "size_mb" : 332,
      "row_count" : 478615
    } ],
    "uid" : "01999ff1-0c6b-741c-8c10-785ddb53d859",
    "type" : "file"
  }, {
    "$type" : "json",
    "parameters" : {
      "type" : "json",
      "file_path" : "C:\\Projects\\datapipeline\\src\\Server\\Services\\DataProcessing\\API\\files\\01999ff1-0c6c-76a7-a6bb-0c1ceefd654d\\part1.json"
    },
    "schema_infos" : [ {
      "fields_count" : 39,
      "fields" : [ {
        "name" : "date_exec",
        "path" : "date_exec",
        "data_type" : "date",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2024-10-09", "2024-10-08", "2024-10-07", "2024-10-06", "2024-10-04", "2024-10-05", "2024-10-03", "2024-10-02", "2024-10-01", "2024-09-30" ],
        "unique_values" : 15,
        "null_count" : 0,
        "statistics" : {
          "min" : "2024-09-26",
          "max" : "2024-10-10",
          "avg" : ""
        }
      }, {
        "name" : "code_form_ind_entrep",
        "path" : "code_form_ind_entrep",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1", "2" ],
        "unique_values" : 2,
        "null_count" : 0,
        "statistics" : {
          "min" : "1",
          "max" : "2",
          "avg" : "1.01"
        }
      }, {
        "name" : "name_form_ind_entrep",
        "path" : "name_form_ind_entrep",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Индивидуальный предприниматель", "Глава крестьянского (фермерского) хозяйства" ],
        "unique_values" : 2,
        "null_count" : 0,
        "statistics" : {
          "min" : "30",
          "max" : "43",
          "avg" : "30.09"
        }
      }, {
        "name" : "inf_surname_ind_entrep_sex",
        "path" : "inf_surname_ind_entrep_sex",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1", "2" ],
        "unique_values" : 2,
        "null_count" : 0,
        "statistics" : {
          "min" : "1",
          "max" : "2",
          "avg" : "1.44"
        }
      }, {
        "name" : "citizenship_kind",
        "path" : "citizenship_kind",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1", "2", "3" ],
        "unique_values" : 3,
        "null_count" : 0,
        "statistics" : {
          "min" : "1",
          "max" : "3",
          "avg" : "1.02"
        }
      }, {
        "name" : "inf_authority_reg_ind_entrep_name",
        "path" : "inf_authority_reg_ind_entrep_name",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Управление Федеральной налоговой службы по Пензенской области", "Межрайонная инспекция Федеральной налоговой службы №10 по Ленинградской области", "Управление Федеральной налоговой службы по Мурманской области", "Межрайонная инспекция Федеральной налоговой службы №15 по Санкт-Петербургу", "Межрайонная инспекция Федеральной налоговой службы № 16 по Краснодарскому краю", "Межрайонная инспекция Федеральной налоговой службы № 46 по г. Москве", "Управление Федеральной налоговой службы по Республике Калмыкия", "Межрайонная инспекция Федеральной налоговой службы №23 по Московской области", "Межрайонная инспекция Федеральной налоговой службы № 17 по Пермскому краю", "Управление Федеральной налоговой службы по Удмуртской Республике" ],
        "unique_values" : 89,
        "null_count" : 0,
        "statistics" : {
          "min" : "57",
          "max" : "102",
          "avg" : "72.57"
        }
      }, {
        "name" : "inf_authority_reg_ind_entrep_code",
        "path" : "inf_authority_reg_ind_entrep_code",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "5800", "4704", "5100", "7847", "2375", "7746", "0800", "5081", "5958", "1800" ],
        "unique_values" : 89,
        "null_count" : 0,
        "statistics" : {
          "min" : "100",
          "max" : "9901",
          "avg" : "4846.32"
        }
      }, {
        "name" : "inf_reg_tax_ind_entrep",
        "path" : "inf_reg_tax_ind_entrep",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Управление Федеральной налоговой службы по Пензенской области", "Межрайонная инспекция Федеральной налоговой службы №2 по Ленинградской области", "Межрайонная инспекция Федеральной налоговой службы №7 по Ленинградской области", "Межрайонная инспекция Федеральной налоговой службы №3 по Ленинградской области", "Межрайонная инспекция Федеральной налоговой службы №9 по Ленинградской области", "Межрайонная инспекция Федеральной налоговой службы №10 по Ленинградской области", "Межрайонная инспекция Федеральной налоговой службы №26 по Санкт-Петербургу", "Межрайонная инспекция Федеральной налоговой службы №20 по Санкт-Петербургу", "Инспекция Федеральной налоговой службы по федеральной территории \"Сириус\"", "Инспекция Федеральной налоговой службы № 7 по г. Москве" ],
        "unique_values" : 378,
        "null_count" : 7,
        "statistics" : {
          "min" : "54",
          "max" : "152",
          "avg" : "70.63"
        }
      }, {
        "name" : "inf_okved_code",
        "path" : "inf_okved_code",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "01.25.1", "43.39", "43.29", "47.99.2", "49.41.3", "69.10", "47.91", "62.01", "56.10", "47.11.2" ],
        "unique_values" : 1395,
        "null_count" : 5,
        "statistics" : {
          "min" : "2",
          "max" : "8",
          "avg" : "5.36"
        }
      }, {
        "name" : "inf_okved_name",
        "path" : "inf_okved_name",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Выращивание прочих плодовых и ягодных культур", "Производство прочих отделочных и завершающих работ", "Производство прочих строительно-монтажных работ", "Деятельность по осуществлению торговли через автоматы", "Аренда грузового автомобильного транспорта с водителем", "Деятельность в области права", "Торговля розничная по почте или по информационно-коммуникационной сети Интернет", "Разработка компьютерного программного обеспечения", "Деятельность ресторанов и услуги по доставке продуктов питания", "Торговля розничная незамороженными продуктами, включая напитки и табачные изделия, в неспециализированных магазинах" ],
        "unique_values" : 1365,
        "null_count" : 5,
        "statistics" : {
          "min" : "11",
          "max" : "359",
          "avg" : "68.33"
        }
      }, {
        "name" : "process_dttm",
        "path" : "process_dttm",
        "data_type" : "date",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2024-10-09T21:07:09.765+03:00", "2024-10-09T21:07:09.693+03:00", "2024-10-09T21:07:10.613+03:00", "2024-10-09T21:07:10.451+03:00", "2024-10-09T21:07:10.434+03:00", "2024-10-09T21:07:10.522+03:00", "2024-10-09T21:07:12.813+03:00", "2024-10-09T21:07:12.768+03:00", "2024-10-09T21:07:14.195+03:00", "2024-10-09T21:07:14.092+03:00" ],
        "unique_values" : 4013,
        "null_count" : 0,
        "statistics" : {
          "min" : "2024-10-09",
          "max" : "2024-10-10",
          "avg" : ""
        }
      }, {
        "name" : "error_code",
        "path" : "error_code",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0" ],
        "unique_values" : 1,
        "null_count" : 0,
        "statistics" : {
          "min" : "0",
          "max" : "0",
          "avg" : "0.00"
        }
      }, {
        "name" : "inf_surname_ind_entrep_firstname",
        "path" : "inf_surname_ind_entrep_firstname",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "ВЛАДИМИР", "МАКСИМ", "ШЕРЗОД", "АНДРЕЙ", "НАДЕЖДА", "АНТОН", "ЕВГЕНИЯ", "АННА", "ИГНАТ", "ОЛЬГА" ],
        "unique_values" : 3240,
        "null_count" : 0,
        "statistics" : {
          "min" : "1",
          "max" : "32",
          "avg" : "6.40"
        }
      }, {
        "name" : "inf_surname_ind_entrep_surname",
        "path" : "inf_surname_ind_entrep_surname",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "ЗВЕРЕВ", "БАЛУЕВ", "ЧАГОЧКИН", "АНАТОЛИЙ", "СИМОНОВА", "САРВАР", "КАДЫР АДЖИЕВА", "ГОЛЬНЕВА", "СЕМАК", "НАУМОВА" ],
        "unique_values" : 13631,
        "null_count" : 2,
        "statistics" : {
          "min" : "2",
          "max" : "32",
          "avg" : "7.75"
        }
      }, {
        "name" : "inf_surname_ind_entrep_midname",
        "path" : "inf_surname_ind_entrep_midname",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "ЕВГЕНЬЕВИЧ", "АЛЕКСАНДРОВИЧ", "ЮРЬЕВИЧ", "СОЛИЖОНОВИЧ", "АЛЕКСАНДРОВНА", "АНДРЕЕВИЧ", "ГАЗИССОВНА", "ЗЕЛИМХАНОВНА", "ВЛАДИМИРОВИЧ", "МИХАЙЛОВНА" ],
        "unique_values" : 3084,
        "null_count" : 5420,
        "statistics" : {
          "min" : "1",
          "max" : "20",
          "avg" : "10.06"
        }
      }, {
        "name" : "dob",
        "path" : "dob",
        "data_type" : "date",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1980-02-29", "1985-08-01", "1968-10-07", "2002-08-06", "2000-06-29", "2004-01-20", "2004-03-08", "1955-07-14", "1985-01-03", "1969-11-15" ],
        "unique_values" : 18937,
        "null_count" : 0,
        "statistics" : {
          "min" : "1934-12-07",
          "max" : "2011-04-12",
          "avg" : ""
        }
      }, {
        "name" : "date_ogrnip",
        "path" : "date_ogrnip",
        "data_type" : "date",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2023-11-14", "2025-01-02", "2020-02-26", "2025-08-27", "2025-05-24", "2023-07-13", "2025-09-03", "2023-05-18", "2021-07-11", "2022-06-23" ],
        "unique_values" : 7400,
        "null_count" : 0,
        "statistics" : {
          "min" : "2003-01-28",
          "max" : "2025-10-09",
          "avg" : ""
        }
      }, {
        "name" : "id_card",
        "path" : "id_card",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "77 82 349990", "503461406", "34 68 741072", "17 44 613831", "65 42 900138", "28 65 806254", "22 12 577664", "04 54 546163", "74 25 065739", "40 35 551374" ],
        "unique_values" : 93868,
        "null_count" : 0,
        "statistics" : {
          "min" : "7",
          "max" : "17",
          "avg" : "11.94"
        }
      }, {
        "name" : "innfl",
        "path" : "innfl",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "301127163757", "606163038359", "886737772797", "336958249166", "200732654883", "967614018290", "853817729471", "005460653363", "559811342296", "502906100091" ],
        "unique_values" : 93868,
        "null_count" : 0,
        "statistics" : {
          "min" : "1333015",
          "max" : "999995777542",
          "avg" : "500544954074.34"
        }
      }, {
        "name" : "ogrnip",
        "path" : "ogrnip",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "865363287164043", "633724687498507", "031125119070468", "778634764239044", "045318327750391", "620460690811947", "088566189403394", "682619088089904", "269397963479312", "096011251468511" ],
        "unique_values" : 93868,
        "null_count" : 0,
        "statistics" : {
          "min" : "44477449990",
          "max" : "999996115827028",
          "avg" : "500890454916052.35"
        }
      }, {
        "name" : "attr_ГРНИП",
        "path" : "inf_okved_opt.ГРНИПДата.attr_ГРНИП",
        "data_type" : "integer",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "501892037413202", "196664199989513", "665461963844494", "925317896914629", "425913637743225", "320958036448400", "532301901197505", "565853419854311", "607268604179460", "137336685050655" ],
        "unique_values" : 739963,
        "null_count" : 20781,
        "statistics" : {
          "min" : "505865380",
          "max" : "999998180656543",
          "avg" : "499807130670956.04"
        }
      }, {
        "name" : "attr_ДатаЗаписи",
        "path" : "inf_okved_opt.ГРНИПДата.attr_ДатаЗаписи",
        "data_type" : "date",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2024-06-07", "2023-12-20", "2023-05-14", "2024-03-23", "2023-08-20", "2024-02-04", "2021-06-05", "2019-11-13", "2023-08-06", "2022-12-27" ],
        "unique_values" : 8243,
        "null_count" : 20781,
        "statistics" : {
          "min" : "2003-01-30",
          "max" : "2025-10-10",
          "avg" : ""
        }
      }, {
        "name" : "attr_КодОКВЭД",
        "path" : "inf_okved_opt.attr_КодОКВЭД",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "38.49", "94.01", "81.42", "23.20", "67.10", "68.34", "32.72", "73.77", "29.73", "50.28" ],
        "unique_values" : 134539,
        "null_count" : 20781,
        "statistics" : {
          "min" : "2",
          "max" : "8",
          "avg" : "5.62"
        }
      }, {
        "name" : "attr_НаимОКВЭД",
        "path" : "inf_okved_opt.attr_НаимОКВЭД",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Предоставление услуг в области растениеводства", "Торговля оптовая цветами и растениями", "Прочие виды полиграфической деятельности", "Изготовление печатных форм и подготовительная деятельность", "Деятельность брошюровочно- переплетная и отделочная и сопутствующие услуги", "Ремонт металлоизделий", "Ремонт машин и оборудования", "Ремонт и техническое обслуживание прочих транспортных средств и оборудования", "Строительство жилых и нежилых зданий", "Разборка и снос зданий" ],
        "unique_values" : 2401,
        "null_count" : 20781,
        "statistics" : {
          "min" : "11",
          "max" : "359",
          "avg" : "65.56"
        }
      }, {
        "name" : "attr_ПрВерсОКВЭД",
        "path" : "inf_okved_opt.attr_ПрВерсОКВЭД",
        "data_type" : "integer",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2014" ],
        "unique_values" : 1,
        "null_count" : 20781,
        "statistics" : {
          "min" : "2014",
          "max" : "2014",
          "avg" : "2014.00"
        }
      }, {
        "name" : "insured_pf",
        "path" : "insured_pf",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "305481831532", "247538274786", "483204952719", "935286375735", "951085989403", "378722936277", "774863602732", "438016722006", "226318069927", "179629897472" ],
        "unique_values" : 64128,
        "null_count" : 29740,
        "statistics" : {
          "min" : "11",
          "max" : "12",
          "avg" : "12.00"
        }
      }, {
        "name" : "email_ind_entrep",
        "path" : "email_ind_entrep",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "zverev.vladimir_syn_7298@YANDEX.RU", "chagochkin.sherzod_syn_5414@GMAIL.COM", "aandrei_4_syn_3305@MAIL.RU", "sarvar.anton_93_syn_975@YANDEX.RU", "kevgeniya_syn_1148@MAIL.RU", "ganna_syn_4670@MAIL.RU", "signat_30_syn_5256@GMAIL.COM", "naumova.olga_syn_3734@YANDEX.RU", "migor_syn_7598@YANDEX.RU", "gadzhiev.bagaveev_syn_1543@BK.RU" ],
        "unique_values" : 73430,
        "null_count" : 20431,
        "statistics" : {
          "min" : "17",
          "max" : "60",
          "avg" : "30.92"
        }
      }, {
        "name" : "citizenship_name",
        "path" : "citizenship_name",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "ТАДЖИКИСТАН", "УЗБЕКИСТАН", "ТУРКМЕНИЯ", "ПАКИСТАН", "УКРАИНА", "СИРИЯ", "КИТАЙ", "АЗЕРБАЙДЖАН", "АРМЕНИЯ", "КАЗАХСТАН" ],
        "unique_values" : 54,
        "null_count" : 92090,
        "statistics" : {
          "min" : "3",
          "max" : "36",
          "avg" : "9.17"
        }
      }, {
        "name" : "id_card_alien_for_rus",
        "path" : "id_card_alien_for_rus",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "32№2986662", "40№2974004", "28№8649028", "84 № 6657250", "834522802", "22№1235803", "90996/6650/07", "40№9179410", "458165290", "35№8081325" ],
        "unique_values" : 1785,
        "null_count" : 92083,
        "statistics" : {
          "min" : "2",
          "max" : "17",
          "avg" : "10.49"
        }
      }, {
        "name" : "inf_stop_ind_entrep_name",
        "path" : "inf_stop_ind_entrep_name",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Индивидуальный предприниматель прекратил деятельность в связи с принятием им соответствующего решения", "Недействующий ИП исключен из ЕГРИП", "Крестьянское (фермерское) хозяйство прекратило деятельность по решению членов крестьянского (фермерского) хозяйства", "Индивидуальный предприниматель прекратил деятельность в связи со смертью", "Прекратил деятельность в связи с окончанием срока действия документа, подтверждающего право временно или постоянно проживать в Российской Федерации", "Прекратил деятельность в связи с принятием судом решения о признании его несостоятельным (банкротом)", "Утратил государственную регистрацию в качестве индивидуального предпринимателя на основании статьи 3 ФЗ от 23.06.2003 №76-ФЗ", "Регистрация индивидуального предпринимателя признана недействительной (ошибочной) на основании решения регистрирующего органа", "Государственная регистрация физического лица в качестве индивидуального предпринимателя признана недействительной в соответствии с пунктом 4 статьи 22.1 Федерального закона от 8 августа 2001 года № 129-ФЗ", "Регистрация индивидуального предпринимателя признана недействительной на основании решения суда" ],
        "unique_values" : 12,
        "null_count" : 73366,
        "statistics" : {
          "min" : "34",
          "max" : "204",
          "avg" : "93.47"
        }
      }, {
        "name" : "inf_stop_ind_entrep_code",
        "path" : "inf_stop_ind_entrep_code",
        "data_type" : "integer",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "201", "209", "301", "202", "207", "203", "501", "801", "601", "701" ],
        "unique_values" : 12,
        "null_count" : 73366,
        "statistics" : {
          "min" : "201",
          "max" : "801",
          "avg" : "203.10"
        }
      }, {
        "name" : "inf_status_ind_entrep_name",
        "path" : "inf_status_ind_entrep_name",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Принято решение о предстоящем исключении недействующего ИП из ЕГРИП", "Принято решение о предстоящем исключении ИП из ЕГРИП (пп. «б» п.2 Закона № 129-ФЗ)", "Глава крестьянского (фермерского) хозяйства отсутствует в связи со смертью", "Глава крестьянского (фермерского) хозяйства отсутствует в связи с окончанием срока действия документа, подтверждающего право временно или постоянно проживать в Российской Федерации" ],
        "unique_values" : 4,
        "null_count" : 86560,
        "statistics" : {
          "min" : "67",
          "max" : "180",
          "avg" : "69.89"
        }
      }, {
        "name" : "inf_status_ind_entrep_code",
        "path" : "inf_status_ind_entrep_code",
        "data_type" : "integer",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "110", "111", "101", "104" ],
        "unique_values" : 4,
        "null_count" : 86560,
        "statistics" : {
          "min" : "101",
          "max" : "111",
          "avg" : "110.17"
        }
      }, {
        "name" : "insuref_fss",
        "path" : "insuref_fss",
        "data_type" : "integer",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "173856101397129", "856471362694236", "305234329718883", "266127184658282", "238605160848353", "740512230323047", "429890519780869", "421446452434033", "174022224941036", "861187631360731" ],
        "unique_values" : 10019,
        "null_count" : 83849,
        "statistics" : {
          "min" : "8189870741",
          "max" : "999943803414630",
          "avg" : "500846367569877.02"
        }
      }, {
        "name" : "inf_reg_ind_entrep_num",
        "path" : "inf_reg_ind_entrep_num",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "ССА № 6", "99/05237", "93/51166", "74/5274", "53/40090", "26206", "48294", "54177", "5163", "М 304" ],
        "unique_values" : 1650,
        "null_count" : 92159,
        "statistics" : {
          "min" : "1",
          "max" : "21",
          "avg" : "6.34"
        }
      }, {
        "name" : "inf_reg_ind_entrep_date",
        "path" : "inf_reg_ind_entrep_date",
        "data_type" : "date",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1996-07-05", "1995-04-27", "1999-08-30", "1997-07-02", "1999-04-04", "2002-12-13", "2001-04-20", "1998-03-20", "2002-08-18", "2000-11-22" ],
        "unique_values" : 1364,
        "null_count" : 92159,
        "statistics" : {
          "min" : "1991-12-29",
          "max" : "2004-12-08",
          "avg" : ""
        }
      }, {
        "name" : "inf_surname_ind_entrep_zags_surname",
        "path" : "inf_surname_ind_entrep_zags_surname",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "ШТУКЕРТ", "МЕЗЕНЦЕВА", "ЗАВАРЗИНА", "БАБИЧ", "ТУМАШЕВА", "ПЕТРОВСКАЯ", "ХУСНУТДИНОВА", "САДЛОВСКАЯ", "ОРАТОВСКАЯ", "АКУЛОВА" ],
        "unique_values" : 975,
        "null_count" : 92737,
        "statistics" : {
          "min" : "2",
          "max" : "18",
          "avg" : "8.13"
        }
      }, {
        "name" : "inf_surname_ind_entrep_zags_firstname",
        "path" : "inf_surname_ind_entrep_zags_firstname",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "СОФЬЯ", "ЛЮДМИЛА", "ТАТЬЯНА", "АЛЕВТИНА", "АНАСТАСИЯ", "ВИКТОРИЯ", "МАРИНА", "МАРИЯ", "ФИДАНИЯ", "ГАЛИНА" ],
        "unique_values" : 293,
        "null_count" : 92737,
        "statistics" : {
          "min" : "1",
          "max" : "16",
          "avg" : "6.33"
        }
      }, {
        "name" : "inf_surname_ind_entrep_zags_midname",
        "path" : "inf_surname_ind_entrep_zags_midname",
        "data_type" : "string",
        "nullable" : true,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "АЛЕКСАНДРОВНА", "ВЛАДИМИРОВНА", "ХАБИНУРОВНА", "АЛЕКСЕЕВНА", "БОРИСОВНА", "ВАЛЕРЬЕВНА", "СЕРГЕЕВНА", "ГРИГОРЬЕВНА", "АНДРЕЕВНА", "ВЛАДИСЛАВОВНА" ],
        "unique_values" : 253,
        "null_count" : 92790,
        "statistics" : {
          "min" : "1",
          "max" : "16",
          "avg" : "10.13"
        }
      } ],
      "size_mb" : 414,
      "row_count" : 93868
    } ],
    "uid" : "01999ff1-37c7-76fb-bc46-5324aff1b037",
    "type" : "file"
  }, {
    "$type" : "xml",
    "parameters" : {
      "type" : "xml",
      "file_path" : "C:\\Projects\\datapipeline\\src\\Server\\Services\\DataProcessing\\API\\files\\01999ff1-37c8-765d-ac5d-98af6207e09a\\part1.xml"
    },
    "schema_infos" : [ {
      "element_count" : 114,
      "elements" : [ {
        "name" : "letter",
        "path" : "dated_info.letter",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Л", "А, А1, Г, 2, 1, 3, Г1", "Г", "А, А1, А2, А., Г, Г1, Г2, Г3, 1, 2", "А, Г, Г1, Г2, Г4, Г5, Г6, Г7, Г9, Н, Н1, У, 1, 2, 3, 4, 5, к", "Б-Б1-Б2-б", "А, А1, А2, А3, а, а1", "А", "А, А1", "А-а1" ],
        "unique_values" : 14219,
        "null_count" : -189441,
        "statistics" : {
          "min" : "1",
          "max" : "100",
          "avg" : "7.61"
        }
      }, {
        "name" : "last_container_fixed_at",
        "path" : "last_container_fixed_at",
        "data_type" : "date",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2019-01-09T00:00:00+03:00", "2021-12-26T12:46:59+00:00", "2023-03-24T13:04:32+00:00", "2022-01-11T07:12:55+00:00", "2021-12-26T12:32:54+00:00", "2022-09-12T08:24:56+00:00", "2025-03-12T10:22:24+00:00", "2021-07-09T14:40:57+00:00", "2022-11-27T20:27:12+00:00", "2022-12-20T15:19:04+00:00" ],
        "unique_values" : 75113,
        "null_count" : -189441,
        "statistics" : {
          "min" : "1111-11-11",
          "max" : "2025-08-22",
          "avg" : ""
        }
      }, {
        "name" : "status",
        "path" : "status",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "actual", "archived" ],
        "unique_values" : 2,
        "null_count" : -189441,
        "statistics" : {
          "min" : "6",
          "max" : "8",
          "avg" : "6.09"
        }
      }, {
        "name" : "previously_posted",
        "path" : "previously_posted",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "true", "false", "True", "False" ],
        "unique_values" : 4,
        "null_count" : -138345,
        "statistics" : {
          "min" : "4",
          "max" : "5",
          "avg" : "4.03"
        }
      }, {
        "name" : "quarter_cad_number",
        "path" : "quarter_cad_number",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "77:17:0140116", "77:17:0140207", "77:22:0040136", "77:20:0020425", "77:01:0006026", "77:20:0020441", "77:08:0009023", "77:17:0000000", "77:17:0110601", "77:17:0110114" ],
        "unique_values" : 2966,
        "null_count" : -189441,
        "statistics" : {
          "min" : "13",
          "max" : "13",
          "avg" : "13.00"
        }
      }, {
        "name" : "value",
        "path" : "value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Здание", "Металлические", "Каменные и бетонные", "Наземный контур", "Деревянные", "Нежилое", "Кирпичные", "Из мелких бетонных блоков", "Из прочих материалов", "Полигон" ],
        "unique_values" : 52,
        "null_count" : -821159,
        "statistics" : {
          "min" : "4",
          "max" : "55",
          "avg" : "24.77"
        }
      }, {
        "name" : "name",
        "path" : "name",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "холодильный цех-ангар", "жилой дом", "Садовый дом", "гаражный бокс", "жилое строение (садовый дом)", "дом", "Экспериментальный завод модульного домостроения с АБК", "нежилое здание", "здание БКТП", "Здание проходной № 2" ],
        "unique_values" : 11081,
        "null_count" : -134328,
        "statistics" : {
          "min" : "1",
          "max" : "642",
          "avg" : "16.07"
        }
      }, {
        "name" : "value",
        "path" : "permitted_use.value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Нежилое", "Жилое", "Многоквартирный дом", "Жилой дом", "Гараж", "Жилое строение", "Садовый дом" ],
        "unique_values" : 7,
        "null_count" : -156612,
        "statistics" : {
          "min" : "5",
          "max" : "19",
          "avg" : "7.87"
        }
      }, {
        "name" : "registration_date",
        "path" : "registration_date",
        "data_type" : "date",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2013-01-08T18:10:58+04:00", "2013-01-08T00:00:00+04:00", "2022-08-29T14:18:10+00:00", "2013-01-08T17:14:54+04:00", "2012-05-22T19:16:51+04:00", "2013-01-08T18:22:56+04:00", "2012-05-23T15:44:15+04:00", "2013-01-08T18:13:56+04:00", "2013-10-21T11:44:01+04:00", "2013-10-21T11:44:57+04:00" ],
        "unique_values" : 54366,
        "null_count" : -189441,
        "statistics" : {
          "min" : "1111-11-11",
          "max" : "2025-08-22",
          "avg" : ""
        }
      }, {
        "name" : "section_number",
        "path" : "section_number",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "50:21:0140218:1614", "50:21:0140207:1336", "77:22:0040136:399", "50:27:0020425:309", "77:01:0006026:1168", "50:21:0110101:389", "77:08:0009023:1010", "50:21:0120111:1006", "77:17:0140308:330", "77:17:0110114:176" ],
        "unique_values" : 151153,
        "null_count" : -189441,
        "statistics" : {
          "min" : "15",
          "max" : "20",
          "avg" : "17.45"
        }
      }, {
        "name" : "floors",
        "path" : "floors",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1", "8", "2", "4", "9", "6", "5", "3", "12", "7" ],
        "unique_values" : 483,
        "null_count" : -179484,
        "statistics" : {
          "min" : "1",
          "max" : "40",
          "avg" : "1.09"
        }
      }, {
        "name" : "year_built",
        "path" : "year_built",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2008", "2022", "1987", "1959", "2009", "1948", "1950", "2010", "1972", "1912" ],
        "unique_values" : 312,
        "null_count" : -135316,
        "statistics" : {
          "min" : "1",
          "max" : "28",
          "avg" : "4.00"
        }
      }, {
        "name" : "sk_id",
        "path" : "contour.sk_id",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "МСК г. Москвы", "ПМСК Москвы", "МСК Москвы", "ПМСК-Москвы", "МСК-77", "МСК- Москвы", "ск кадастрового округа", "СК Московская", "МСК-50, зона 2", "\"ПМСК Москвы\"" ],
        "unique_values" : 81,
        "null_count" : -139062,
        "statistics" : {
          "min" : "1",
          "max" : "29",
          "avg" : "10.95"
        }
      }, {
        "name" : "value",
        "path" : "contour.value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "МСК Москвы", "ПМСК Москвы", "Метод спутниковых геодезических измерений (определений)", "Аналитический метод", "Наземный контур", "МСК-50, зона 1", "Полигон", "Геодезический", "МСК-50, зона 2", "Подземный контур" ],
        "unique_values" : 11,
        "null_count" : -139062,
        "statistics" : {
          "min" : "7",
          "max" : "55",
          "avg" : "10.66"
        }
      }, {
        "name" : "value",
        "path" : "spatials_elements.value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Полигон", "Наземный контур", "Подземный контур", "Надземный контур", "Полилиния" ],
        "unique_values" : 5,
        "null_count" : -69760,
        "statistics" : {
          "min" : "7",
          "max" : "16",
          "avg" : "9.80"
        }
      }, {
        "name" : "x",
        "path" : "ordinates.x",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "-33138.93", "-5040.4", "-5039.72", "-17417.23", "-21499.78", "-700.23", "-19978.69", "-20011.78", "-20009.02", "-20018.17" ],
        "unique_values" : 37639,
        "null_count" : -73038,
        "statistics" : {
          "min" : "-43781.49",
          "max" : "2197329.04",
          "avg" : "-6461.94"
        }
      }, {
        "name" : "y",
        "path" : "ordinates.y",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "-49744.99", "-1645.13", "-1646.71", "-8305.94", "-37719.76", "-22509.87", "-7264.49", "-31467.1", "-31476.24", "-31471.38" ],
        "unique_values" : 37636,
        "null_count" : -73038,
        "statistics" : {
          "min" : "-56435.56",
          "max" : "474691.46",
          "avg" : "-8351.69"
        }
      }, {
        "name" : "ord_nmb",
        "path" : "ordinates.ord_nmb",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1" ],
        "unique_values" : 1,
        "null_count" : -73038,
        "statistics" : {
          "min" : "1",
          "max" : "1",
          "avg" : "1.00"
        }
      }, {
        "name" : "num_geopoint",
        "path" : "ordinates.num_geopoint",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1", "23", "37", "51", "21", "5", "22", "33", "7", "25" ],
        "unique_values" : 1690,
        "null_count" : -73038,
        "statistics" : {
          "min" : "1",
          "max" : "5",
          "avg" : "1.73"
        }
      }, {
        "name" : "delta_geopoint",
        "path" : "ordinates.delta_geopoint",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0.1", "1", "0.08", "0.01", "0.07", "0.06", "0.09", "0.2", "0", "0.14" ],
        "unique_values" : 20,
        "null_count" : -73038,
        "statistics" : {
          "min" : "0",
          "max" : "8",
          "avg" : "0.10"
        }
      }, {
        "name" : "y",
        "path" : "y",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "-49742.82", "-49745.73", "-49747.91", "-49744.99", "8810.34", "8779.79", "8786.59", "8805.34", "8809.49", "14198.94" ],
        "unique_values" : 1210520,
        "null_count" : -3000843,
        "statistics" : {
          "min" : "-56628.04",
          "max" : "6150170.75",
          "avg" : "1488.87"
        }
      }, {
        "name" : "ord_nmb",
        "path" : "ord_nmb",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2", "3", "4", "1", "5", "6", "7", "8", "9", "10" ],
        "unique_values" : 2017,
        "null_count" : -2375106,
        "statistics" : {
          "min" : "1",
          "max" : "2017",
          "avg" : "67.55"
        }
      }, {
        "name" : "num_geopoint",
        "path" : "num_geopoint",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2", "3", "4", "1", "5", "6", "7", "8", "9", "10" ],
        "unique_values" : 14697,
        "null_count" : -2365884,
        "statistics" : {
          "min" : "1",
          "max" : "5",
          "avg" : "2.43"
        }
      }, {
        "name" : "delta_geopoint",
        "path" : "delta_geopoint",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0.1", "0.08", "0.01", "1", "0.2", "0.07", "0.14", "0.06", "0", "0.5" ],
        "unique_values" : 32,
        "null_count" : -1090068,
        "statistics" : {
          "min" : "0",
          "max" : "13.1",
          "avg" : "0.10"
        }
      }, {
        "name" : "invent_cost",
        "path" : "dated_info.invent_cost",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "34844", "20170050.4", "2105622", "4735936.62", "126758", "85542308.83", "12265050.85", "469373.96", "23085", "19913" ],
        "unique_values" : 48461,
        "null_count" : -189441,
        "statistics" : {
          "min" : "0",
          "max" : "3725083103.45",
          "avg" : "19539514.09"
        }
      }, {
        "name" : "value",
        "path" : "contour.spatials_elements.value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Полигон", "Наземный контур", "Полилиния", "Надземный контур" ],
        "unique_values" : 4,
        "null_count" : -139062,
        "statistics" : {
          "min" : "7",
          "max" : "16",
          "avg" : "7.00"
        }
      }, {
        "name" : "ord_nmb",
        "path" : "contour.ordinates.ord_nmb",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1", "2" ],
        "unique_values" : 2,
        "null_count" : -139062,
        "statistics" : {
          "min" : "1",
          "max" : "2",
          "avg" : "1.00"
        }
      }, {
        "name" : "x",
        "path" : "contour.ordinates.x",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "10549.5", "-514.12", "20337.5", "9334.55", "8524.1", "10204.85", "-19191.75", "-12501.37", "17238.3", "6476.35" ],
        "unique_values" : 33831,
        "null_count" : -139062,
        "statistics" : {
          "min" : "-43185.19",
          "max" : "3173047.6",
          "avg" : "5404.07"
        }
      }, {
        "name" : "y",
        "path" : "contour.ordinates.y",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "8809.49", "14196.09", "14848.67", "10926.04", "-10156.52", "7367.78", "-26500", "-5593.29", "5065.34", "4494.83" ],
        "unique_values" : 33866,
        "null_count" : -139062,
        "statistics" : {
          "min" : "-55816.64",
          "max" : "6150170.75",
          "avg" : "8570.89"
        }
      }, {
        "name" : "z",
        "path" : "contour.ordinates.z",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0", "1" ],
        "unique_values" : 2,
        "null_count" : -139062,
        "statistics" : {
          "min" : "0",
          "max" : "1",
          "avg" : "0.00"
        }
      }, {
        "name" : "x",
        "path" : "contour.x",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "10544.05", "-512.19", "20347.37", "9331.4", "14865.45", "-19411.71", "8528.77", "10206.87", "-20129.19", "-19183.68" ],
        "unique_values" : 55404,
        "null_count" : -139062,
        "statistics" : {
          "min" : "-43785.7",
          "max" : "3173058.75",
          "avg" : "-648.56"
        }
      }, {
        "name" : "y",
        "path" : "contour.y",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "8817.14", "14197.86", "14846.13", "10924.54", "6539.54", "-9441.56", "-10156.84", "7368.42", "-25734.26", "-26500.34" ],
        "unique_values" : 56234,
        "null_count" : -139062,
        "statistics" : {
          "min" : "-56621.02",
          "max" : "6150173.44",
          "avg" : "95.83"
        }
      }, {
        "name" : "z",
        "path" : "contour.z",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0", "0.1", "2", "3" ],
        "unique_values" : 4,
        "null_count" : -139062,
        "statistics" : {
          "min" : "0",
          "max" : "3",
          "avg" : "0.00"
        }
      }, {
        "name" : "x",
        "path" : "x",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "10533.85", "10555.45", "10565.65", "10552.4", "10549.5", "-512.76", "-511.87", "-510.79", "-509.33", "-499.68" ],
        "unique_values" : 890104,
        "null_count" : -1923500,
        "statistics" : {
          "min" : "-43788.66",
          "max" : "3173065.95",
          "avg" : "1121.93"
        }
      }, {
        "name" : "z",
        "path" : "z",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0", "0.1", "3", "4", "5", "6", "1" ],
        "unique_values" : 7,
        "null_count" : -498678,
        "statistics" : {
          "min" : "0",
          "max" : "6",
          "avg" : "0.00"
        }
      }, {
        "name" : "underground_floors",
        "path" : "underground_floors",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1", "0", "2", "0-1", "3", "7", "-", "подвал", "4", "подвад" ],
        "unique_values" : 66,
        "null_count" : -46608,
        "statistics" : {
          "min" : "1",
          "max" : "41",
          "avg" : "1.07"
        }
      }, {
        "name" : "year_commisioning",
        "path" : "year_commisioning",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1959", "1948", "1972", "1912", "2023", "1966", "2017", "1984", "1956", "2006" ],
        "unique_values" : 315,
        "null_count" : -63820,
        "statistics" : {
          "min" : "1",
          "max" : "28",
          "avg" : "4.00"
        }
      }, {
        "name" : "record_number",
        "path" : "record_number",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "50:21:0120111:1006", "50:26:0190401:613", "50:26:0140401:1324", "50:27:0000000:47694", "50:27:0040308:147", "77:03:0006017:1062", "50:27:0000000:45159", "50:26:0190806:65", "77:02:0024001:1030", "50:27:0000000:6413" ],
        "unique_values" : 8494,
        "null_count" : -8772,
        "statistics" : {
          "min" : "15",
          "max" : "20",
          "avg" : "17.79"
        }
      }, {
        "name" : "code",
        "path" : "entity_spatial.code",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "77.1", "77.0", "50.2", "00.0", "0.0", "77.2", "77.9", "-", "77", "50.1" ],
        "unique_values" : 14,
        "null_count" : -62147,
        "statistics" : {
          "min" : "1",
          "max" : "4",
          "avg" : "4.00"
        }
      }, {
        "name" : "value",
        "path" : "entity_spatial.value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "ПМСК Москвы", "МСК Москвы", "МСК-50, зона 2", "МСК-50, зона 1", "Подземный контур", "Наземный контур", "Надземный контур" ],
        "unique_values" : 7,
        "null_count" : -62147,
        "statistics" : {
          "min" : "10",
          "max" : "16",
          "avg" : "10.47"
        }
      }, {
        "name" : "number_pp",
        "path" : "number_pp",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" ],
        "unique_values" : 76,
        "null_count" : -13927,
        "statistics" : {
          "min" : "1",
          "max" : "76",
          "avg" : "3.40"
        }
      }, {
        "name" : "name",
        "path" : "material.name",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "жилой дом", "Жилой дом", "Дом", "Жилое строение без права регистрации проживания, расположенное на дачном земельном участке", "Хозблок", "нежилое здание", "Садовый дом", "Склад", "Баня -прачечная", "Здание служебно-эксплуатационного блока" ],
        "unique_values" : 2319,
        "null_count" : -189439,
        "statistics" : {
          "min" : "1",
          "max" : "250",
          "avg" : "17.94"
        }
      }, {
        "name" : "value",
        "path" : "material.value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Жилое", "Нежилое", "Жилой дом", "Гараж", "Многоквартирный дом", "Жилое строение" ],
        "unique_values" : 6,
        "null_count" : -189439,
        "statistics" : {
          "min" : "5",
          "max" : "19",
          "avg" : "6.52"
        }
      }, {
        "name" : "ord_nmb",
        "path" : "contour.spatials_elements.ordinates.ord_nmb",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1", "2", "3" ],
        "unique_values" : 3,
        "null_count" : -139062,
        "statistics" : {
          "min" : "1",
          "max" : "3",
          "avg" : "1.00"
        }
      }, {
        "name" : "x",
        "path" : "contour.spatials_elements.ordinates.x",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "14867.9", "-19419.02", "-20134.21", "-39888.45", "-21573.54", "-27179.02", "-13312.67", "-7847.69", "-8575.59", "-42857.55" ],
        "unique_values" : 22546,
        "null_count" : -139062,
        "statistics" : {
          "min" : "-43788.66",
          "max" : "3173047.6",
          "avg" : "-8999.96"
        }
      }, {
        "name" : "y",
        "path" : "contour.spatials_elements.ordinates.y",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "6552.89", "-9438.13", "-25727.46", "-23314.47", "-16059.77", "-47588.7", "-29412.84", "-33044.35", "-31842.93", "-26139.31" ],
        "unique_values" : 22555,
        "null_count" : -139062,
        "statistics" : {
          "min" : "-56616.29",
          "max" : "6150170.75",
          "avg" : "-11586.83"
        }
      }, {
        "name" : "z",
        "path" : "contour.spatials_elements.ordinates.z",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0", "0.1", "1" ],
        "unique_values" : 3,
        "null_count" : -139062,
        "statistics" : {
          "min" : "0",
          "max" : "1",
          "avg" : "0.00"
        }
      }, {
        "name" : "x",
        "path" : "contour.spatials_elements.x",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "14861.84", "-19411.51", "-20128.35", "-39883.8", "-21561.34", "-27170.97", "-13303.69", "-7840.97", "-8572.93", "-42849.63" ],
        "unique_values" : 22219,
        "null_count" : -139062,
        "statistics" : {
          "min" : "-43786.4",
          "max" : "3173055.36",
          "avg" : "-9070.51"
        }
      }, {
        "name" : "y",
        "path" : "contour.spatials_elements.y",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "6550.99", "-9438.54", "-25731.6", "-23319.9", "-16057.4", "-47585.16", "-29413.86", "-33048.73", "-31844.48", "-26143.75" ],
        "unique_values" : 22468,
        "null_count" : -139062,
        "statistics" : {
          "min" : "-56621.89",
          "max" : "6150173.44",
          "avg" : "-11589.30"
        }
      }, {
        "name" : "z",
        "path" : "contour.spatials_elements.z",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0", "0.1", "2" ],
        "unique_values" : 3,
        "null_count" : -139062,
        "statistics" : {
          "min" : "0",
          "max" : "2",
          "avg" : "0.00"
        }
      }, {
        "name" : "delta_geopoint",
        "path" : "contour.spatials_elements.ordinates.delta_geopoint",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0.1", "0.06", "0.08", "0.07", "0.09", "1", "0", "0.2", "0.05", "0.01" ],
        "unique_values" : 21,
        "null_count" : -139062,
        "statistics" : {
          "min" : "-0.01",
          "max" : "7.5",
          "avg" : "0.10"
        }
      }, {
        "name" : "num_geopoint",
        "path" : "contour.spatials_elements.ordinates.num_geopoint",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1", "н 1", "7", "6", "8", "14", "5", "67", "3", "37" ],
        "unique_values" : 63,
        "null_count" : -139062,
        "statistics" : {
          "min" : "1",
          "max" : "4",
          "avg" : "1.02"
        }
      }, {
        "name" : "num_geopoint",
        "path" : "contour.spatials_elements.num_geopoint",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2", "7", "213", "н 2", "1", "24", "8", "92", "9", "15" ],
        "unique_values" : 73,
        "null_count" : -139062,
        "statistics" : {
          "min" : "1",
          "max" : "4",
          "avg" : "1.02"
        }
      }, {
        "name" : "ord_nmb",
        "path" : "contour.spatials_elements.ord_nmb",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2" ],
        "unique_values" : 1,
        "null_count" : -139062,
        "statistics" : {
          "min" : "2",
          "max" : "2",
          "avg" : "2.00"
        }
      }, {
        "name" : "num_geopoint",
        "path" : "contour.num_geopoint",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "3", "2", "8", "214", "н 3", "7", "25", "9", "93", "1" ],
        "unique_values" : 111,
        "null_count" : -139062,
        "statistics" : {
          "min" : "1",
          "max" : "4",
          "avg" : "1.03"
        }
      }, {
        "name" : "ord_nmb",
        "path" : "contour.ord_nmb",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "3", "2" ],
        "unique_values" : 2,
        "null_count" : -139062,
        "statistics" : {
          "min" : "2",
          "max" : "3",
          "avg" : "2.90"
        }
      }, {
        "name" : "delta_geopoint",
        "path" : "contour.ordinates.delta_geopoint",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0.08", "0.1", "0.03", "0.07", "0.06", "0.09", "0.2", "0.01", "0.05", "0" ],
        "unique_values" : 13,
        "null_count" : -139062,
        "statistics" : {
          "min" : "0",
          "max" : "1",
          "avg" : "0.10"
        }
      }, {
        "name" : "num_geopoint",
        "path" : "contour.ordinates.num_geopoint",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1", "4", "22", "21", "8", "25", "7", "15", "2", "17" ],
        "unique_values" : 38,
        "null_count" : -139062,
        "statistics" : {
          "min" : "1",
          "max" : "3",
          "avg" : "1.03"
        }
      }, {
        "name" : "delta_geopoint",
        "path" : "contour.delta_geopoint",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0.08", "0.1", "1", "0.09", "0.05", "0.03", "0.07", "0.01", "0.2", "0" ],
        "unique_values" : 10,
        "null_count" : -139062,
        "statistics" : {
          "min" : "0",
          "max" : "1",
          "avg" : "0.10"
        }
      }, {
        "name" : "definition",
        "path" : "definition",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Здание 1(1)", "Здание 1(2)", "ЗД1(1)", "ЗД1(2)", "1", "2", "(2)", "3", "4", "5" ],
        "unique_values" : 2727,
        "null_count" : -50963,
        "statistics" : {
          "min" : "1",
          "max" : "49",
          "avg" : "3.55"
        }
      }, {
        "name" : "num_geopoint",
        "path" : "spatial_element.num_geopoint",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "5", "7", "21", "82", "86", "92", "96", "108", "167", "191" ],
        "unique_values" : 732,
        "null_count" : -25594,
        "statistics" : {
          "min" : "1",
          "max" : "5",
          "avg" : "2.08"
        }
      }, {
        "name" : "ord_nmb",
        "path" : "spatial_element.ord_nmb",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1" ],
        "unique_values" : 1,
        "null_count" : -25594,
        "statistics" : {
          "min" : "1",
          "max" : "1",
          "avg" : "1.00"
        }
      }, {
        "name" : "x",
        "path" : "spatial_element.x",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "-39892.11", "-8572.93", "9283.38", "9268.23", "9269.16", "9301.94", "9291.18", "9242.36", "-4146.43", "8267.01" ],
        "unique_values" : 9874,
        "null_count" : -25594,
        "statistics" : {
          "min" : "-42887.58",
          "max" : "28661.7",
          "avg" : "-2770.59"
        }
      }, {
        "name" : "y",
        "path" : "spatial_element.y",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "-23317.61", "-31844.48", "4887.46", "4819.19", "4742.78", "4768.16", "4890.76", "4872.98", "4872.96", "-15370.38" ],
        "unique_values" : 9868,
        "null_count" : -25594,
        "statistics" : {
          "min" : "-56536.56",
          "max" : "37983.13",
          "avg" : "-2208.02"
        }
      }, {
        "name" : "sk_id",
        "path" : "sk_id",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "МСК Москвы", "ПМСК Москвы", "ПМСК  Москвы", "ПМСК МОСКВЫ", "МСК Москвы -77.1", "МСК- Москвы", "ПМСК Москвы, зона 1", "МСК-50", "Московская", "МСК г.Москвы" ],
        "unique_values" : 70,
        "null_count" : -10518,
        "statistics" : {
          "min" : "3",
          "max" : "28",
          "avg" : "10.46"
        }
      }, {
        "name" : "value",
        "path" : "ordinates.value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Геодезический", "Метод спутниковых геодезических измерений (определений)", "Аналитический метод", "Картометрический", "Иное описание" ],
        "unique_values" : 5,
        "null_count" : -73038,
        "statistics" : {
          "min" : "13",
          "max" : "55",
          "avg" : "49.74"
        }
      }, {
        "name" : "code",
        "path" : "object_formation.code",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "9", "1", "Раздел", "Иное", "2" ],
        "unique_values" : 5,
        "null_count" : -189441,
        "statistics" : {
          "min" : "1",
          "max" : "6",
          "avg" : "1.02"
        }
      }, {
        "name" : "value",
        "path" : "object_formation.value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Иное", "Раздел", "Выдел" ],
        "unique_values" : 3,
        "null_count" : -189441,
        "statistics" : {
          "min" : "4",
          "max" : "6",
          "avg" : "5.13"
        }
      }, {
        "name" : "z",
        "path" : "spatial_element.z",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0" ],
        "unique_values" : 1,
        "null_count" : -25594,
        "statistics" : {
          "min" : "0",
          "max" : "0",
          "avg" : "0.00"
        }
      }, {
        "name" : "code",
        "path" : "spatial_element.code",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "01", "2", "0", "1" ],
        "unique_values" : 4,
        "null_count" : -25594,
        "statistics" : {
          "min" : "0",
          "max" : "2",
          "avg" : "1.01"
        }
      }, {
        "name" : "value",
        "path" : "spatial_element.value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Полигон", "Надземный контур", "Наземный контур", "Подземный контур", "Метод спутниковых геодезических измерений (определений)" ],
        "unique_values" : 5,
        "null_count" : -25594,
        "statistics" : {
          "min" : "7",
          "max" : "55",
          "avg" : "7.75"
        }
      }, {
        "name" : "z",
        "path" : "ordinates.z",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0" ],
        "unique_values" : 1,
        "null_count" : -73038,
        "statistics" : {
          "min" : "0",
          "max" : "0",
          "avg" : "0.00"
        }
      }, {
        "name" : "delta_geopoint",
        "path" : "spatials_elements.ordinates.delta_geopoint",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1", "0.1" ],
        "unique_values" : 2,
        "null_count" : -69760,
        "statistics" : {
          "min" : "0.1",
          "max" : "1",
          "avg" : "0.98"
        }
      }, {
        "name" : "ord_nmb",
        "path" : "spatials_elements.ordinates.ord_nmb",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1" ],
        "unique_values" : 1,
        "null_count" : -69760,
        "statistics" : {
          "min" : "1",
          "max" : "1",
          "avg" : "1.00"
        }
      }, {
        "name" : "x",
        "path" : "spatials_elements.ordinates.x",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "-17521.79", "-17806.3", "-40436.05", "-2303.42", "-2015.42", "-18223.96", "-8188.75", "-17596.98", "-40198.63", "-3089.5" ],
        "unique_values" : 763,
        "null_count" : -69760,
        "statistics" : {
          "min" : "-41262.69",
          "max" : "22324.72",
          "avg" : "-11860.11"
        }
      }, {
        "name" : "y",
        "path" : "spatials_elements.ordinates.y",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "-5631.64", "-6566.81", "-28825.88", "-14645.45", "-4520.16", "-6544.19", "-16429.24", "-6682.73", "-28290.92", "-4583.35" ],
        "unique_values" : 763,
        "null_count" : -69760,
        "statistics" : {
          "min" : "-41673.7",
          "max" : "32397.85",
          "avg" : "-10705.43"
        }
      }, {
        "name" : "ord_nmb",
        "path" : "spatials_elements.ord_nmb",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2" ],
        "unique_values" : 1,
        "null_count" : -69760,
        "statistics" : {
          "min" : "2",
          "max" : "2",
          "avg" : "2.00"
        }
      }, {
        "name" : "x",
        "path" : "spatials_elements.x",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "-17513.55", "-17803.46", "-40433.24", "-2304.92", "-2014.51", "-18220.04", "-8197.18", "-17591.92", "-40204.29", "-3081.46" ],
        "unique_values" : 760,
        "null_count" : -69760,
        "statistics" : {
          "min" : "-41258.29",
          "max" : "22280",
          "avg" : "-11835.27"
        }
      }, {
        "name" : "y",
        "path" : "spatials_elements.y",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "-5628.02", "-6564.58", "-28838.57", "-14631.04", "-4524.46", "-6553.21", "-16441.65", "-6689.39", "-28281.38", "-4583.33" ],
        "unique_values" : 761,
        "null_count" : -69760,
        "statistics" : {
          "min" : "-41676.36",
          "max" : "32397.6",
          "avg" : "-10717.98"
        }
      }, {
        "name" : "delta_geopoint",
        "path" : "contour.spatials_elements.delta_geopoint",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0.1", "1", "0.08", "0.05", "0.01", "0.2" ],
        "unique_values" : 6,
        "null_count" : -139062,
        "statistics" : {
          "min" : "0.01",
          "max" : "1",
          "avg" : "0.10"
        }
      }, {
        "name" : "value",
        "path" : "contour.spatials_elements.ordinates.value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Метод спутниковых геодезических измерений (определений)", "Аналитический метод", "Геодезический" ],
        "unique_values" : 3,
        "null_count" : -139062,
        "statistics" : {
          "min" : "13",
          "max" : "55",
          "avg" : "48.85"
        }
      }, {
        "name" : "code",
        "path" : "actual_unverified_data.code",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "204001000000", "204003000000", "204002000000" ],
        "unique_values" : 3,
        "null_count" : -189441,
        "statistics" : {
          "min" : "204001000000",
          "max" : "204003000000",
          "avg" : "204001636029.41"
        }
      }, {
        "name" : "value",
        "path" : "actual_unverified_data.value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Нежилое", "Многоквартирный дом", "Жилое" ],
        "unique_values" : 3,
        "null_count" : -189441,
        "statistics" : {
          "min" : "5",
          "max" : "19",
          "avg" : "7.85"
        }
      }, {
        "name" : "value",
        "path" : "contour.ordinates.value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Метод спутниковых геодезических измерений (определений)", "Геодезический", "Аналитический метод" ],
        "unique_values" : 3,
        "null_count" : -139062,
        "statistics" : {
          "min" : "13",
          "max" : "55",
          "avg" : "44.86"
        }
      }, {
        "name" : "invent_date",
        "path" : "dated_info.invent_date",
        "data_type" : "date",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2006-07-19", "2011-03-11", "2005-07-08", "2000-09-07", "2010-12-10" ],
        "unique_values" : 5,
        "null_count" : -189441,
        "statistics" : {
          "min" : "2000-09-07",
          "max" : "2011-03-11",
          "avg" : ""
        }
      }, {
        "name" : "name_oti",
        "path" : "dated_info.name_oti",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "ГУП МосгорБТИ", "Подольский филиал,", "None", "Московский городской филиал ФГУП \"Ростехинвентаризация-Федеральное БТИ\"" ],
        "unique_values" : 4,
        "null_count" : -189441,
        "statistics" : {
          "min" : "4",
          "max" : "71",
          "avg" : "21.43"
        }
      }, {
        "name" : "underground_floors",
        "path" : "purpose.underground_floors",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1", "0", "2", "None" ],
        "unique_values" : 4,
        "null_count" : -5619,
        "statistics" : {
          "min" : "1",
          "max" : "4",
          "avg" : "1.20"
        }
      }, {
        "name" : "value",
        "path" : "name.value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Гараж", "Жилой дом", "Нежилое" ],
        "unique_values" : 3,
        "null_count" : -134336,
        "statistics" : {
          "min" : "5",
          "max" : "9",
          "avg" : "6.25"
        }
      }, {
        "name" : "delta_geopoint",
        "path" : "additional_contour_location.intersection_point.delta_geopoint",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0.1" ],
        "unique_values" : 1,
        "null_count" : -2,
        "statistics" : {
          "min" : "0.1",
          "max" : "0.1",
          "avg" : "0.10"
        }
      }, {
        "name" : "depth",
        "path" : "additional_contour_location.intersection_point.depth",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0" ],
        "unique_values" : 1,
        "null_count" : -2,
        "statistics" : {
          "min" : "0",
          "max" : "0",
          "avg" : "0.00"
        }
      }, {
        "name" : "height",
        "path" : "additional_contour_location.intersection_point.height",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0" ],
        "unique_values" : 1,
        "null_count" : -2,
        "statistics" : {
          "min" : "0",
          "max" : "0",
          "avg" : "0.00"
        }
      }, {
        "name" : "x",
        "path" : "additional_contour_location.intersection_point.x",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "11421.17" ],
        "unique_values" : 1,
        "null_count" : -2,
        "statistics" : {
          "min" : "11421.17",
          "max" : "11421.17",
          "avg" : "11421.17"
        }
      }, {
        "name" : "y",
        "path" : "additional_contour_location.intersection_point.y",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "10519.53" ],
        "unique_values" : 1,
        "null_count" : -2,
        "statistics" : {
          "min" : "10519.53",
          "max" : "10519.53",
          "avg" : "10519.53"
        }
      }, {
        "name" : "temporary_info",
        "path" : "temporary_info",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "false", "False", "true" ],
        "unique_values" : 3,
        "null_count" : -9,
        "statistics" : {
          "min" : "4",
          "max" : "5",
          "avg" : "4.67"
        }
      }, {
        "name" : "code",
        "path" : "ordinate.code",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "692005000000", "692006000000" ],
        "unique_values" : 2,
        "null_count" : -2999237,
        "statistics" : {
          "min" : "692005000000",
          "max" : "692006000000",
          "avg" : "692005029411.76"
        }
      }, {
        "name" : "value",
        "path" : "ordinate.value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Метод спутниковых геодезических измерений (определений)", "Аналитический метод" ],
        "unique_values" : 2,
        "null_count" : -2999237,
        "statistics" : {
          "min" : "19",
          "max" : "55",
          "avg" : "53.94"
        }
      }, {
        "name" : "value",
        "path" : "spatials_elements.ordinates.value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Метод спутниковых геодезических измерений (определений)" ],
        "unique_values" : 1,
        "null_count" : -69760,
        "statistics" : {
          "min" : "55",
          "max" : "55",
          "avg" : "55.00"
        }
      }, {
        "name" : "num_geopoint",
        "path" : "spatials_elements.num_geopoint",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1" ],
        "unique_values" : 1,
        "null_count" : -69760,
        "statistics" : {
          "min" : "1",
          "max" : "1",
          "avg" : "1.00"
        }
      }, {
        "name" : "code",
        "path" : "entity_spatial.spatial_element.code",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "01" ],
        "unique_values" : 1,
        "null_count" : -62147,
        "statistics" : {
          "min" : "1",
          "max" : "1",
          "avg" : "1.00"
        }
      }, {
        "name" : "value",
        "path" : "entity_spatial.spatial_element.value",
        "data_type" : "string",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "Полигон" ],
        "unique_values" : 1,
        "null_count" : -62147,
        "statistics" : {
          "min" : "7",
          "max" : "7",
          "avg" : "7.00"
        }
      }, {
        "name" : "x",
        "path" : "entity_spatial.ordinates.x",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "-6444.83" ],
        "unique_values" : 1,
        "null_count" : -62147,
        "statistics" : {
          "min" : "-6444.83",
          "max" : "-6444.83",
          "avg" : "-6444.83"
        }
      }, {
        "name" : "y",
        "path" : "entity_spatial.ordinates.y",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "12822.15" ],
        "unique_values" : 1,
        "null_count" : -62147,
        "statistics" : {
          "min" : "12822.15",
          "max" : "12822.15",
          "avg" : "12822.15"
        }
      }, {
        "name" : "ord_nmb",
        "path" : "entity_spatial.ordinates.ord_nmb",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1" ],
        "unique_values" : 1,
        "null_count" : -62147,
        "statistics" : {
          "min" : "1",
          "max" : "1",
          "avg" : "1.00"
        }
      }, {
        "name" : "y",
        "path" : "entity_spatial.y",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "12828.19", "639.2" ],
        "unique_values" : 2,
        "null_count" : -62147,
        "statistics" : {
          "min" : "639.2",
          "max" : "12828.19",
          "avg" : "8765.19"
        }
      }, {
        "name" : "ord_nmb",
        "path" : "entity_spatial.ord_nmb",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2" ],
        "unique_values" : 1,
        "null_count" : -62147,
        "statistics" : {
          "min" : "2",
          "max" : "2",
          "avg" : "2.00"
        }
      }, {
        "name" : "delta_geopoint",
        "path" : "spatial_element.delta_geopoint",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0.1" ],
        "unique_values" : 1,
        "null_count" : -25594,
        "statistics" : {
          "min" : "0.1",
          "max" : "0.1",
          "avg" : "0.10"
        }
      }, {
        "name" : "max_depth",
        "path" : "additional_contour_location.max_depth",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "0" ],
        "unique_values" : 1,
        "null_count" : -2,
        "statistics" : {
          "min" : "0",
          "max" : "0",
          "avg" : "0.00"
        }
      }, {
        "name" : "max_height",
        "path" : "additional_contour_location.max_height",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "12" ],
        "unique_values" : 1,
        "null_count" : -2,
        "statistics" : {
          "min" : "12",
          "max" : "12",
          "avg" : "12.00"
        }
      }, {
        "name" : "num_geopoint",
        "path" : "entity_spatial.spatial_element.num_geopoint",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1" ],
        "unique_values" : 1,
        "null_count" : -62147,
        "statistics" : {
          "min" : "1",
          "max" : "1",
          "avg" : "1.00"
        }
      }, {
        "name" : "ord_nmb",
        "path" : "entity_spatial.spatial_element.ord_nmb",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "1" ],
        "unique_values" : 1,
        "null_count" : -62147,
        "statistics" : {
          "min" : "1",
          "max" : "1",
          "avg" : "1.00"
        }
      }, {
        "name" : "x",
        "path" : "entity_spatial.spatial_element.x",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "9202.13" ],
        "unique_values" : 1,
        "null_count" : -62147,
        "statistics" : {
          "min" : "9202.13",
          "max" : "9202.13",
          "avg" : "9202.13"
        }
      }, {
        "name" : "y",
        "path" : "entity_spatial.spatial_element.y",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "644.25" ],
        "unique_values" : 1,
        "null_count" : -62147,
        "statistics" : {
          "min" : "644.25",
          "max" : "644.25",
          "avg" : "644.25"
        }
      }, {
        "name" : "num_geopoint",
        "path" : "entity_spatial.num_geopoint",
        "data_type" : "integer",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "2" ],
        "unique_values" : 1,
        "null_count" : -62147,
        "statistics" : {
          "min" : "2",
          "max" : "2",
          "avg" : "2.00"
        }
      }, {
        "name" : "x",
        "path" : "entity_spatial.x",
        "data_type" : "float",
        "nullable" : false,
        "filter" : null,
        "sort" : null,
        "sample_values" : [ "9206.46" ],
        "unique_values" : 1,
        "null_count" : -62147,
        "statistics" : {
          "min" : "9206.46",
          "max" : "9206.46",
          "avg" : "9206.46"
        }
      } ],
      "size_mb" : 937,
      "row_count" : 0
    } ],
    "uid" : "01999ff1-a267-735f-9d52-268094eb3ccb",
    "type" : "file"
  } ],
  "targets" : [ {
    "$type" : "database",
    "parameters" : {
      "type" : "postgre_s_q_l",
      "host" : "localhost",
      "port" : 5432,
      "database" : "test",
      "user" : "postgres",
      "password" : "password",
      "schema" : "public"
    },
    "schema_infos" : [ ],
    "uid" : "01999ff1-a268-7652-912e-6118d87da343",
    "type" : "database"
  } ]
}
```


3) финальный dag файл нужно разбить, что бы я сохранил как отдельные пайпланы. Необходимо также передать связи.

как я это вижу: 
```json
{
    "template": "{код DAG файла без функций}",
    "pipelines": [
        {
            "level": "{номер от 0, где 0 - это первые фукнции по очереди выполнения, 1 - вторые и т.д.}",
            "id": "{uid}",
            "from": "{uid откуда идет связь (если из источника, то uid источника)}",
            "to": "{uid куда идет связь (если в таргет, то uid таргета)}",
            "funtion_name": "create_target_schema",
            "function_name_ru": "Создание схемы в таргете",
            "function_body": "{сама функция с ее объявлением}"
        }
    ]
}
```

4) развернут спарк, по возможности, можно попробовать распределить функции на него, что бы распределенно выполнялись (очень по желанию)