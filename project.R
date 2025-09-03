# Загрузка библиотек
library(tibble)
library(dplyr)
library(purrr)
library(stringr)
library(httr)
library(jsonlite)
library(glue)
library(lubridate)
library(tidyr)
library(openxlsx)

# Функция для запроса данных по государственным и муниципальным контрактам с защитой
data_json <- function(name_firm) {
  product_name <- name_firm
  url <- "http://openapi.clearspending.ru/restapi/v3/contracts/search/"
  perpage <- 50
  max_records <- 500
  pages <- ceiling(max_records / perpage)
  
  all_pages <- list()
  
  for (p in 1:pages) {
    response <- tryCatch(
      GET(url, query = list(
        productsearch = product_name,
        perpage = perpage,
        page = p,
        sort = "-signDate"
      )),
      error = function(e) {
        message("Ошибка запроса для: ", name_firm, " | страница: ", p)
        return(NULL)
      }
    )

    if (is.null(response)) next
    sc <- status_code(response)
    message("Фирма: ", name_firm, " | страница: ", p, " | статус: ", sc)
    
    if (sc == 404) {
      message("Страница не найдена, пропускаем: ", name_firm, " | страница: ", p)
      next
    }
    
    page_data <- tryCatch(
      fromJSON(content(response, "text", encoding = "UTF-8")),
      error = function(e) {
        message("Ошибка парсинга JSON для: ", name_firm, " | страница: ", p)
        return(NULL)
      }
    )
    if (is.null(page_data) || length(page_data$contracts) == 0) break
    
    all_pages[[p]] <- page_data
  }
  
  return(all_pages)
}

list <- c("Холдинг КДВ Групп",
          "Мон'дэлис Русь",
          "Объединённые Кондитеры",
          "Булочно-кондитерский комбинат Коломенский",
          "Рот Фронт",
          "Ладога",
          "Акконд",
          "Невский Кондитер",
          "Пензенская кондитерская фабрика",
          "Конфил",
          "Шоколенд",
          "Конти-Рус",
          "Бабаевский",
          "Приморский кондитер",
          "Эссен Продакшн АГ",
          "Красный Октябрь",
          "Красный Октябрь",
          "Эссен Продакшн АГ",
          "Приморский кондитер",
          "Бабаевский",
          "Конти-Рус",
          "Шоколадная фабрика Новосибирская",
          "Сормовская кондитерская фабрика",
          "Пермская кондитерская фабрика",
          "Кондитерская фабрика Волжанка",
          "КОНДИТЕРСКАЯ ФАБРИКА ИМ. Н.К. КРУПСКОЙ",
          "КДВ Групп",
          "Ферреро Руссия",
          "Кондитерская фабрике 'Нева'",
          "Кондитерская фабрика 'Ударница'",
          "Победа",
          )

# Функция выполняющяя запрос с учётом списка фирм, которые поставляют сладости, сохраняя результат в json формате
response_list_firm <- function() {
  sweet_firms <- c(
    "Ферреро Россия",
    "Кундрат",
    "Кубань Сласть",
    "Славица"
  )
  
  all_firms_data <- list()
  
  for (i in seq_along(sweet_firms)) {
    firm <- sweet_firms[i]
    message("Запрос по фирме: ", firm)
    Sys.sleep(3)  
    all_firms_data[[firm]] <- data_json(firm)
    
    if (i %% 2 == 0) {
      saveRDS(all_firms_data, file = glue("sweet_firms_progress_{i}.rds"))
      message("Промежуточный результат сохранён: sweet_firms_progress_", i, ".rds")
    }
  }
  
  return(all_firms_data)
}

# Вызов функции
contracts_data <- response_list_firm()



# Функция для объединения RDS-файлов, с проверкой по уникальности и сохранением результата
combine_sweet_firms_rds <- function(pattern = "sweet_firms_progress_.*\\.rds$") {
  rds_files <- list.files(pattern = pattern, full.names = TRUE)
  
  if(length(rds_files) == 0) {
    message("Промежуточные файлы не найдены")
    return(list())
  }
  
  all_data <- list()
  for(file in rds_files) {
    file_data <- readRDS(file)
    all_data <- c(all_data, file_data)
  }
  
  unique_firms <- unique(names(all_data))
  final_data <- list()
  
  for(firm in unique_firms) {
    firm_data <- all_data[names(all_data) == firm]
    final_data[[firm]] <- firm_data[[length(firm_data)]]
  }
  
  message("Объединены данные по ", length(final_data), " фирмам")
  saveRDS(final_data, file = "final_data.rds")
  message("Финальные данные сохранены в final_data.rds")

}

# Вызов функции
combine_sweet_firms_rds()







# Функция читает все RDS-файлы в указанной директории
# Преобразует их в единую таблицу с данными о контрактах и продуктах, где каждая строка представляет один конкретный продукт в контракте
read_all_rds_full <- function(path = "data", pattern = "\\.[Rr][Dd][Ss]$", recursive = TRUE) {
  
  rds_files <- list.files(path, pattern = pattern, full.names = TRUE, recursive = recursive)
  if(length(rds_files) == 0) return(tibble())
  
  all_rds <- lapply(rds_files, readRDS)
  
  map_dfr(all_rds, function(rds) {
    map_dfr(names(rds), function(factory_name) {
      factory_list <- rds[[factory_name]]
      
      map_dfr(factory_list, function(contract_elem) {
        contracts <- contract_elem$contracts$data
        if(is.null(contracts)) return(tibble())
        
        products <- contracts$products
        if(is.null(products)) return(tibble())
      
        map_dfr(seq_along(products), function(product_index) {
          prod <- products[[product_index]]
          tibble(
            "Фабрика" = factory_name,
            "Номер контракта" = as.character(contracts$regNum[1]),
            "Дата заключения" = as.Date(contracts$signDate[1]),
            "Федеральный закон" = as.character(contracts$fz[1]),
            "Способ размещения заказа" = as.character(contracts$purchaseInfo$purchaseCodeName[1]),
            "Фирма заказчик" = as.character(contracts$customer$fullName[1]),
            "Фирма изготовитель" = str_extract(prod$name[1], '(?i)\\b(ООО|АО|ТМ|ЗАО|ОАО|ИП|LLC|Inc|S\\.A\\.)\\b.*$'),
            "Срок исполнения контракта" = as.Date(contracts$execution$endDate[1]),
            "Регион" = as.character(contracts$placer$mainInfo$postalAddress[1]),
            "Сумма контракта" = round(as.numeric(contracts$price[1]), 2),
            "Валюта" = as.character(contracts$currency$code[1]),
            "Наименование товара, работ, услуг" = as.character(prod$name[1]),
            "Код продукции" = as.character(if(!is.null(prod$OKPD2)) prod$OKPD2$code[1] else NA),
            "Единицы измерения" = as.character(if(!is.null(prod$OKEI)) prod$OKEI$name[1] else NA),
            "Цена за единицу" = as.numeric(prod$price[1]),
            "Количество" = as.numeric(prod$quantity[1]),
            "Сумма, руб" = {
              price_val <- as.numeric(prod$price[1])
              qty_val <- as.numeric(prod$quantity[1])
              if(any(is.na(c(price_val, qty_val)))) NA else price_val * qty_val},
            "ID продукта" = product_index  
          )
        })
      })
    })
  })
}

# Вызов функции
result_data <- read_all_rds_full(path = "data", pattern = "\\.[Rr][Dd][Ss]$", recursive = TRUE) 




# Функция производит полную очистку и приведение к одному виду
firms_conty <- function(data) {
  if (is.null(data) || nrow(data) == 0) return(tibble(firm_group = character(), n = integer()))
  
  list_firms <- c(
    "Холдинг КДВ Групп", "Мон'дэлис Русь", "Объединённые Кондитеры", 
    "Булочно-кондитерский комбинат Коломенский", "Рот Фронт", "Ладога", 
    "Акконд", "Невский Кондитер", "Пензенская кондитерская фабрика", 
    "Конфил", "Шоколенд", "Конти-Рус", "Бабаевский", "Приморский кондитер", 
    "Эссен Продакшн АГ", "Красный Октябрь", "Шоколадная фабрика Новосибирская", 
    "Сормовская кондитерская фабрика", "Пермская кондитерская фабрика", 
    "Кондитерская фабрика Волжанка", "КОНДИТЕРСКАЯ ФАБРИКА ИМ. Н.К. КРУПСКОЙ", 
    "КДВ Групп", "Ферреро Руссия", "Кондитерская фабрике 'Нева'", 
    "Кондитерская фабрика 'Ударница'", "Победа"
  )
  
  list_firms_clean <- str_to_upper(list_firms) %>% str_trim() %>% unique()
  
  # Функция очистки и группировки
  clean_firms <- function(df) {
    df %>%
      mutate(
        firm_clean = `Фирма изготовитель` %>%
          str_to_upper() %>%
          str_replace_all('["«»]', '') %>%
          str_replace_all('\\..*$', '') %>%
          str_trim() %>%
          str_extract('(АО|ООО|ЗАО|ОАО|ПАО|ИП|LLC|INC|S\\.A\\.|ТМ)\\s+[А-ЯA-Z0-9\\s\\-\\,]+')
      ) %>%
      filter(
        !is.na(firm_clean),
        str_detect(firm_clean, regex(paste(list_firms_clean, collapse = "|")))
      ) %>%
      mutate(
        firm_base = firm_clean %>%
          str_replace_all("РОССИЙСКАЯ ФЕДЕРАЦИЯ", "") %>%
          str_replace_all("ФИРМЕННОЕ НАИМЕНОВАНИЕ.*", "") %>%
          str_replace_all("ТУ.*", "") %>%
          str_replace_all("ЗА \\d{4} ГОД", "") %>%
          str_replace_all("СТРАНА ПРОИСХОЖДЕНИЯ", "") %>%
          str_replace_all("Г$", "") %>%
          str_squish()
      ) %>%
      mutate(
        firm_group = case_when(
          str_detect(firm_base, "КОНТИ") ~ "КОНТИ-РУС",
          str_detect(firm_base, "ЭССЕН") ~ "ЭССЕН ПРОДАКШН АГ",
          str_detect(firm_base, "ФЕРРЕРО") ~ "ФЕРРЕРО РОССИЯ",
          str_detect(firm_base, "СОРМОВСКАЯ") ~ "СОРМОВСКАЯ КОНДИТЕРСКАЯ ФАБРИКА",
          str_detect(firm_base, "ШОКОЛАДНАЯ") ~ "ШОКОЛАДНАЯ ФАБРИКА НОВОСИБИРСКАЯ",
          str_detect(firm_base, "ПЕНЗЕНСКАЯ") ~ "ПЕНЗЕНСКАЯ КОНДИТЕРСКАЯ ФАБРИКА",
          str_detect(firm_base, "ВОЛЖАНКА") ~ "КОНДИТЕРСКАЯ ФАБРИКА ВОЛЖАНКА",
          str_detect(firm_base, "РОТ ФРОНТ") ~ "РОТ ФРОНТ",
          str_detect(firm_base, "АККОНД") ~ "АККОНД",
          str_detect(firm_base, "НЕВСКИЙ КОНДИТЕР") ~ "НЕВСКИЙ КОНДИТЕР",
          str_detect(firm_base, "ПРИМОРСКИЙ КОНДИТЕР") ~ "ПРИМОРСКИЙ КОНДИТЕР",
          str_detect(firm_base, "КДВ") ~ "КДВ ГРУПП",
          TRUE ~ str_trim(firm_base)
        )
      ) %>% 
      select(-firm_base, -firm_base, -firm_clean, -Фабрика, -`Фирма изготовитель`)
  }
  clean_firms(data)
}

# Вызов функции
result_contracts <- firms_conty(result_data)


# Функция на вход получает датафрейм, удаляет символы в названии фирмы в колонке "Фирма изготовитель"
# Считает финансовые метрики, динамику по месяцам
calc_metrics <- function(data, big_contract_threshold = 1e7) {
  if (is.null(data)) {
    message("Данные отсутствуют!")
    return(NULL)
  }
  
  # Создание переменной месяц
  data <- data %>% 
    mutate(
      month = month(`Дата заключения`)
    )
  
  # Метрики по фирмам
  firm_metrics <- data %>%
    filter(!is.na(firm_group)) %>%
    group_by(firm_group) %>%
    summarise(
      sum_total    = sum(`Сумма, руб`, na.rm = TRUE),
      mean_total   = mean(`Сумма, руб`, na.rm = TRUE),
      median_total = median(`Сумма, руб`, na.rm = TRUE),
      max_total    = max(`Сумма, руб`, na.rm = TRUE),
      big_share    = mean(`Сумма, руб` > big_contract_threshold, na.rm = TRUE) * 100,
      .groups = "drop"
    )
  
  # Динамика по месяцам 
  monthly_metrics <- data %>%
    filter(!is.na(firm_group)) %>%
    group_by(firm_group, month) %>%
    summarise(
      sum_month = sum(`Сумма, руб`, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    group_by(firm_group) %>%
    mutate(
      month_growth = ifelse(!is.na(lag(sum_month)) & lag(sum_month) > 0,
                            (sum_month / lag(sum_month) - 1) * 100,
                            NA)
    ) %>%
    arrange(firm_group, month)
  
  # Метрики по времени
  time_metrics <- data %>% 
    filter(!is.na(firm_group)) %>% 
    group_by(firm_group, month) %>% 
    summarise(
      mean_num_contract = n_distinct(`Номер контракта`, na.rm = TRUE),
      mean_duration_contracts = as.numeric(mean(`Срок исполнения контракта` - `Дата заключения`, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    ungroup() %>% 
    arrange(firm_group, month)
  
  # Кто чаще всего заказывает в какие месяцы, какие товары
  frequency_customer <- data %>% 
    filter(!is.na(`Фирма заказчик`)) %>% 
    group_by(`Фирма заказчик`, month, `Код продукции`) %>% 
    summarise(
      count_firm_customer = n_distinct(`Номер контракта`, na.rm = TRUE),
      .groups = "drop"
    ) %>% 
    arrange(desc(count_firm_customer))
  
  # Конкуренция
  competition_contracts <- data %>% 
    filter(!is.na(`Номер контракта`)) %>% 
    group_by(`Номер контракта`) %>%
    summarise(
      mean_competitor = n_distinct(firm_group),
      .groups = "drop"
    ) %>% 
    arrange(desc(mean_competitor))
  
  list(
    firms = firm_metrics,
    monthly = monthly_metrics,
    time = time_metrics,
    frequency = frequency_customer,
    competition = competition_contracts
  )
}

# Вызов функции
metrics_list <- calc_metrics(result_contracts, big_contract_threshold = 1e6)



# Функция убирает все не нужные символы, оставляет только город
extract_city <- function(addr) {
  if(is.na(addr) || addr == "") return(NA_character_)
  
  addr_clean <- toupper(addr) %>%
    str_replace("^\\d{5,6},\\s*", "") %>%   
    str_replace_all("РЕСП|ОБЛ\\.|Р-Н|ТЕР\\.|ВН\\. ТЕР\\.", "") %>% 
    str_squish()
  
  # Сначала ищем "г." и берём все символы до запятой
  city_customer <- str_extract(addr_clean, "Г\\.?\\s*[^,]+")
  
  # Если не найдено, ищем "город" после названия
  if(is.na(city_customer)) {
    city_customer <- str_extract(addr_clean, "[А-ЯЁ\\-\\s]+Город")
  }
  
  city_customer <- city_customer %>%
    str_replace("^Г\\.?\\s*", "") %>%     
    str_replace("\\s*ГОРОД$", "") %>%     
    str_squish()
  
  # Исправление некорректных вариантов
  if(city_customer == "СКАЯ") city_customer <- "ОРЕНБУРГ"
  if(city_customer == "ОРОД") city_customer <- "МОСКВА"
  
  if(city_customer == "") return(NA_character_) else return(city_customer)
}

# Применение функции c дальнейшем подсчетом суммы контрактов для каждого города, с группировкой
result_QGIS <- result_contracts %>%
  mutate(city_customer = sapply(Регион, extract_city)) %>%
  group_by(`Фирма заказчик`, city_customer) %>%
  filter(!is.na(city_customer)) %>% 
  summarise(
    sum_contract = sum(`Сумма контракта`, na.rm = TRUE)
  )


# Сохранение результата для QGIS
write.csv(result_coords, "result_QGIS.csv")

# Сохранение результата для PowerPoint
wb <- createWorkbook()

for(i in seq_along(metrics_list)){
  addWorksheet(wb, paste0("Sheet", i))
  writeData(wb, sheet = i, metrics_list[[i]])
}

saveWorkbook(wb, "metrics_list.xlsx", overwrite = TRUE)
