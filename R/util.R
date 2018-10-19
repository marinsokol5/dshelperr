
concatenate_paths <- function(...) {
  return(paste(..., sep="/"))
}

extract_name_from_path <- function(path) {
  full_file_name <- tail(strsplit(path, "/")[[1]], n=1)
  file_name <- strsplit(full_file_name, "\\.")[[1]][[1]]
  return (file_name)
}


# https://stackoverflow.com/questions/48372955/creating-dummy-variable-based-on-event-date-within-certain-time-period
# https://stackoverflow.com/questions/39554629/how-to-do-non-equi-join-with-variable-column-name
# function assumes that joint variable is id_prikljucek and time variable is mesec
dummy_variable_within_period <- function(left, right, dummy_name, start_date, end_date, join_id="id_prikljucek", time_column="mesec") {
  if (!(is.data.table(left))) {
    setDT(left)
  }
  if (!(is.data.table(right))) {
    setDT(right)
  }

  join_condition = c(
    join_id,
    sprintf("%s>=%s", time_column, start_date),
    sprintf("%s<=%s", time_column, end_date)
  )
  left[, (dummy_name) := 0][right, on = join_condition, (dummy_name) := 1]
  return (left)
}

replace_diacritic_signs <- function(words) {
  words <- gsub("[čć]", "c", words)
  words <- gsub("[ČĆ]", "C", words)
  words <- gsub("[š]", "s", words)
  words <- gsub("[Š]", "S", words)
  words <- gsub("[ž]", "z", words)
  words <- gsub("[Ž]", "Z", words)
  words <- gsub("[đ]", "d", words)
  words <- gsub("[Đ]", "D", words)
  return (words)
}

replace_diacritic_signs_df <- function(dataframe) {
  colnames(dataframe) %<>% replace_diacritic_signs()
  return (dataframe)
}

load_libraries <- function(libraries) {
  not_installed <- libraries[!libraries %in% installed.packages()]
  for(lib in not_installed) install.packages(lib, dependencies = TRUE)
  sapply(libraries, require, character = TRUE)
}

source_files <- function(files) {
  sapply(files, source, echo = FALSE, print.eval = FALSE)
}

apply_on_columns <- function(dataset, columns, apply_function, everyone_but_them = FALSE) {
  if (everyone_but_them) {
    columns <- setdiff(colnames(dataset), columns)
  }
  dataset[columns] <- lapply(dataset[columns], apply_function)
  return (dataset)
}

factorize_columns <- function(dataset, columns, everyone_but_them = FALSE) {
  apply_on_columns(dataset, columns, factor, everyone_but_them)
}

characterize_columns <- function(dataset, columns, everyone_but_them = FALSE){
  apply_on_columns(dataset, columns, as.character, everyone_but_them)
}

integerize_columns <- function(dataset, columns, everyone_but_them = FALSE) {
  apply_on_columns(dataset, columns, as.integer, everyone_but_them)
}
integerize_columns_with_pattern <- function(dataset, pattern, everyone_but_them = FALSE) {
  columns <- get_columns_with_pattern(dataset, pattern)
  apply_on_columns(dataset, columns, as.integer, everyone_but_them)
}

datevize_columns <- function(dataset, columns, everyone_but_them = FALSE) {
  apply_on_columns(dataset, columns, as.Date, everyone_but_them)
}

# All NA or empty string values in given columns set to 0 and everything else to 1.
# they need to be charachters
binary_encode_na_and_empty_strings <- function(dataset, columns_to_encode, all_others=FALSE) {
  if (all_others) {
    columns_to_encode <- setdiff(colnames(dataset), columns_to_encode)
  }

  dataset %<>% characterize_columns(columns_to_encode)

  for (column in columns_to_encode){
    column_quo = string_to_quosure(column)
    dataset %<>% mutate(
      !!column := ifelse(is.na(!!column_quo) | trimws(!!column_quo) == "", 0, 1)
    )
  }

  dataset %<>% integerize_columns(columns_to_encode)

  dataset
}

remove_columns <- function(dataset, ...) {
  dataset_colnames <- colnames(dataset)
  columns_to_remove <- list(...)

  for (c in columns_to_remove) {
    if (c %in% dataset_colnames) {
      dataset %<>% select(-!!c)
    }
  }
  return (dataset)
}



remove_columns_array <- function(dataset, columns_to_remove) {
  dataset_colnames <- colnames(dataset)

  for (c in columns_to_remove) {
    if (c %in% dataset_colnames) {
      dataset %<>% select(-!!c)
    }
  }
  return (dataset)
}


keep_columns <- function(dataset, ...) {
  dataset_colnames <- colnames(dataset)
  columns_to_keep = c(...)
  for (c in dataset_colnames) {
    if (!c %in% columns_to_keep) {
      dataset %<>% select(-!!c)
    }
  }
  return (dataset)
}

keep_columns_array <- function(dataset, array) {
  dataset_colnames <- colnames(dataset)
  columns_to_keep = array
  for (c in dataset_colnames) {
    if (!c %in% columns_to_keep) {
      dataset %<>% select(-!!c)
    }
  }
  return (dataset)
}



set_day_to_first <- function(date) {
  day(date) <- 1
  return (date)
}

# Creates Date column with every day set to first in the month.
# current_date must be a regular variable, not a string
create_date_first_in_the_month <- function(dataset, current_date, new_date="date", date_origin="1899-12-30", replace=TRUE, first_in_the_month=TRUE) {
  current_date <- enquo(current_date)
  dataset %<>% mutate(
    !! new_date := as.Date(as.numeric(!! current_date), origin = date_origin)
  )

  if (first_in_the_month) {
    new_date_quo <- string_to_quosure(new_date)
    dataset %<>% mutate(
      !! new_date := set_day_to_first(!! new_date_quo)
    )
  }

  if (replace & new_date != current_date) {
    dataset %<>% select(-!!current_date)
  }
  return (dataset)
}

# Number of exact duplicates in all columns.
number_of_duplicates <- function(dataset) {
  nrow(dataset) - nrow(dataset %>% distinct())
}

remove_duplicates <- function(dataset) {
  dataset %>% distinct()
}

# After grouping the dataset how many rows were lost in comparison with the original dataset.
number_of_duplicates_by_group <- function(dataset, ...) {
  group <- quos(...)
  nrow(dataset) - nrow(dataset %>% distinct(!!! group))
}

# After grouping the dataset how many rows have more than one value for each of the columns.
number_of_rows_with_duplicates <- function(dataset, ...) {
  return (nrow(get_duplicates(dataset, ...)))
}

# After grouping the dataset, return rows that have more than one value for each of the columns.
get_duplicates <- function(dataset, ...) {
  group <- quos(...)
  tmp <- dataset %>% group_by(!!! group) %>% summarise(n=n())
  return (tmp %>% filter(n > 1))
}

get_duplicates_by_everything <- function(dataset) {
  dataset %>% group_by_at(names(dataset)) %>% summarise(n = n())
}



# Last parameter is the columns to do the group by.
dummies_after_grouping <- function(dataset, dummy_columns, ...) {
  group <- quos(...)
  group_strings <- c(unlist(lapply(group, quo_name)))
  columns_to_use = union(dummy_columns, group_strings)

  dataset %<>% keep_columns_array(unlist(columns_to_use))

  dataset %<>% dummy.data.frame(
    names = dummy_columns,
    sep="."
  )

  dataset %<>%
    group_by(!!! group) %>%
    # summarise_all(max_is_one_wrapper)
    summarise_all(sum)
    # summarise(function(...) {min(sum(...), 1)})
  # dataset[, !colnames(dataset) %in% group_strings] %<>% lapply(max_is_one)

  # Worked so hard for this.
  # dataset %<>% apply_on_elements(
  #   (function (x) min(x, 1)),
  #   group_strings,
  #   all_others = TRUE
  # )

  return(dataset)
}

dummies_and_frequency_after_grouping <- function(dataset, name_for_the_frequency="n", dummy_columns, ...) {
  dummies <- dummies_after_grouping(dataset, dummy_columns, ...)
  frequencies <- frequency_after_grouping(dataset, name_for_the_frequency, ...)

  group <- quos(...)
  group_strings <- c(unlist(lapply(group, quo_name)))

  merge(dummies, frequencies, by=group_strings)
}

frequency_after_grouping <- function(dataset, name_for_the_frequency="n", ...) {
  group <- quos(...)
  group_strings <- c(unlist(lapply(group, quo_name)))

  dataset %<>%
    group_by(!!! group) %>%
    summarise(!! name_for_the_frequency := n())

  return(dataset)
}

max_is_one_wrapper <- function(...) {
  max_is_one(sum(...))
}

max_is_one <- function(number) {
  min(number, 1)
}

# messes up the classes of a dataframe
# https://stackoverflow.com/questions/15215848/apply-function-to-every-value-in-an-r-dataframe
apply_on_elements <- function(dataset, func, columns, all_others=FALSE) {
  if (all_others) {
    condition <- !colnames(dataset) %in% columns
  } else {
    condition <- colnames(dataset) %in% columns
  }

  dataset[, condition] <- as.data.frame(lapply(
    dataset[, condition],
    FUN = function(x) {sapply(x, FUN = func)}
  ))
  return (dataset)
}

trim_whitespaces_in_columns <- function(dataset, columns, all_others = FALSE) {
  # return (apply_on_elements(dataset, trimws, columns, all_others))
  if (all_others) {
    columns <- setdiff(colnames(dataset), columns)
  }
  dataset %<>% rowwise()
  for (col in columns) {
    col_quo <- string_to_quosure(col)
    dataset %<>%
      mutate(!! col := trimws(!! col_quo))
  }
  dataset %>% ungroup() %>% as.data.frame()
}

concatenate_name_before_some_colums <- function(all_columns, name, columns_to_concatenate, ALL_OTHERS=FALSE) {
  if (ALL_OTHERS) {
    condition <- function (x) {!x %in% columns_to_concatenate}
  } else {
    condition <- function (x) {x %in% columns_to_concatenate}
  }
  c(unlist(lapply(
    all_columns,
    function(x) {ifelse(condition(x), paste(name, x, sep="."), x)}
  )))
}

concatenate_name_before_some_colums_df <- function(dataset, name, columns_to_concatenate, ALL_OTHERS=FALSE) {
  all_columns <- colnames(dataset)
  if (ALL_OTHERS) {
    condition <- function (x) {!x %in% columns_to_concatenate}
  } else {
    condition <- function (x) {x %in% columns_to_concatenate}
  }
  new_names <- c(unlist(lapply(
    all_columns,
    function(x) {ifelse(condition(x), paste(name, x, sep="."), x)}
  )))

  colnames(dataset) <- new_names
  return(dataset)
}

fill_NA_from_another_column <- function(dataset, column_with_missing_values, column_for_filling) {
  column_with_missing_values <-  enquo(column_with_missing_values)
  column_with_missing_values_string <- quo_name(column_with_missing_values)
  column_for_filling <- enquo(column_for_filling)

  dataset %>% mutate(
    !! column_with_missing_values_string := if_else(
      is.na(!! column_with_missing_values),
      !! column_for_filling,
      !! column_with_missing_values
    )
  )
}

fill_NA_with_value <-  function(dataset, column_with_missing_values, value) {
  column_with_missing_values <-  enquo(column_with_missing_values)
  column_with_missing_values_string <- quo_name(column_with_missing_values)

  dataset %>% mutate(
    !! column_with_missing_values_string := if_else(
      is.na(!! column_with_missing_values),
      value,
      !! column_with_missing_values
    )
  )
}

fill_NA_with_value_str <-  function(dataset, column_with_missing_values, value) {
  column_qou <- string_to_quosure(column_with_missing_values)

  dataset %>% mutate(
    !! column_with_missing_values := if_else(
      is.na(!! column_qou),
      value,
      !! column_qou
    )
  )
}

fill_NA_with_mean <-  function(dataset, column_with_missing_values) {
  fill_NA_with_value_str(dataset, column_with_missing_values, mean(dataset[[column_with_missing_values]], na.rm = TRUE))
}


fill_NA_with_value_everything <- function(dataset, value) {
  dataset[is.na(dataset)] <- value
  dataset
}

get_columns_with_pattern <- function(dataset, pattern) {
  grep(pattern, colnames(dataset), value=TRUE)
}

fill_NA_with_value_in_pattern_columns <-  function(dataset, pattern, value) {
  columns <- get_columns_with_pattern(dataset, pattern)

  for (column in columns) {
    column_quo <- string_to_quosure(column)
    dataset %<>% mutate(
      !! column := if_else(
        is.na(!! column_quo),
        value,
        !! column_quo
      )
    )
  }

  dataset
}


fill_empty_string_with_value <- function(dataset, column, value) {
  column_quo = string_to_quosure(column)
  dataset  %>% mutate(
    !! column := if_else(!! column_quo == "", value, !! column_quo)
  )
}

# column as string
replace_value_with_another_value <- function(dataset, column, value_to_replace, replacement, all_others=FALSE) {
  replace_group_of_values_with_another_value(dataset, column, c(value_to_replace), replacement, all_others)
}


replace_group_of_values_with_another_value <- function(dataset, column, group_of_values, replacement, all_others=FALSE) {
  column_string <- column
  column <- string_to_quosure(column_string)

  if (all_others) {
    group_of_values <- setdiff(unique(dataset[, column_string]), group_of_values)
  }

  dataset %<>% mutate(
    !! column_string := if_else(
      (!! column) %in% group_of_values,
      replacement,
      !! column
    )
  )

  return (dataset)
}

replace_value_if_contains_pattern_with_another_value <- function(dataset, column, pattern, replacement) {
  column_quo <- string_to_quosure(column)
  dataset %<>% mutate(
    !! column := if_else(
      grepl(pattern, !! column_quo),
      replacement,
      !! column_quo
    )
  )
  return (dataset)
}

replace_pattern_with_another_value <- function(dataset, column, pattern, replacement) {
  column_quo = string_to_quosure(column)
  dataset %>% mutate(
    !! column := gsub(pattern, replacement, !! column_quo)
  )
}

replace_value_with_another_value_vectorized <- function(dataset, columns, value_to_replace, replacement) {
  for(column in columns) {
    dataset %<>% replace_value_with_another_value(column, value_to_replace, replacement)
  }
  return (dataset)
}

replace_values_with_less_than_minimal_frequency <- function(dataset, column, minimal_frequency, replacement) {
  values <- table(dataset[, column])
  values_to_replace <- names(values[values < minimal_frequency])
  replace_group_of_values_with_another_value(dataset, column, values_to_replace, replacement)
}

replace_values_with_lowest_frequency <- function(dataset, column, how_many, replacement) {
  values <- sort(table(dataset[, column]), decreasing = TRUE)
  values_to_replace <- tail(names(values), n = how_many)
  replace_group_of_values_with_another_value(dataset, column, values_to_replace, replacement)
}

remove_columns_with_one_unique_value <- function(dataset, verbose=FALSE) {
  col_to_remove <- c()
  for (c in colnames(dataset)){
    if (dataset[[c]] %>% unique() %>% length() == 1) {
      col_to_remove %<>% append(c)
    }
  }

  if (verbose) {
    print_color(
      blue,
      "Removing these columns: \n",
      vector_to_string(col_to_remove)
    )
  }
  dataset %>% remove_columns_array(col_to_remove)
}

binary_encode_columns <- function(dataset, columns, zero_value, one_value) {
  dataset %<>% replace_value_with_another_value_vectorized(
    columns,
    zero_value,
    "0"
  )
  dataset %<>% replace_value_with_another_value_vectorized(
    columns,
    one_value,
    "1"
  )

  dataset %<>% integerize_columns(columns)
  dataset %<>% integerize_columns(columns)

  return (dataset)
}



difference_in_months <- function(first_date, second_date) {
  interval(first_date, second_date) %/% months(1)
}

difference_in_days <- function(first_date, second_date) {
  as.numeric(difftime(first_date, second_date, "days"), "days")
}

add_months <- function(date, number_of_months) {
  month(date) <- month(date) + number_of_months
  return (date)
}

add_years <- function(date, number_of_years) {
  year(date) <- year(date) + number_of_years
  return (date)
}

reduce_number_of_unique_values_characters <- function(dataset, column, percentage_of_info_to_keep, name_for_the_removed_values, exclude_from_removal=c(), verbose=FALSE) {
  column <- enquo(column)
  column_string <- quo_name(column)
  # dataset %<>% mutate(
  #   !! column_string := as.character(!! column)
  # )

  values = sort(table(dataset[column_string]), decreasing = TRUE)
  number_of_values = length(values)
  total_rows = sum(values)
  values_to_replace <- c()

  for (i in 1:number_of_values) {
    current_rows = sum(values[1:i])
    kept_information <- current_rows / total_rows
    if (kept_information >= percentage_of_info_to_keep) {
      j <- i + 1
      values_to_replace <- names(values)[j:number_of_values]
      values_to_replace <- setdiff(values_to_replace, exclude_from_removal)

      if (verbose) {
        print("Removing these values:")
        print(values_to_replace)
        print("-------------------------------------------------------")
        print("Keeping these values:")
        print(setdiff(names(values), values_to_replace))
      }

      values_to_replace_size <- length(values_to_replace)
      print(paste0(
        "Reduced ",
        values_to_replace_size,
        " values or ",
        round((values_to_replace_size / number_of_values) * 100, 2),
        "% of the original data",
        ", new number of unique values is "
        , i + 1,
        " and percentage of kept information is ",
        round(kept_information * 100, 2),
        "%."
      ))
      break
    }
  }

  dataset %<>% mutate(
    !! column_string := ifelse((!! column) %in% values_to_replace, name_for_the_removed_values, !! column)
  )

  # dataset %<>% mutate(
  #   !! column_string := as.factor(!! column)
  # )

  return (dataset)
}


reduce_number_of_unique_values_characters_fuzzy_matching <- function(dataset, column, max_distance = 0.1, exclude_from_removal=c(), verbose=FALSE, ignore_case = FALSE, min_length = 1) {
  not_checked_values <- names(sort(table(dataset[column]), decreasing = TRUE))
  not_checked_values %<>% setdiff(exclude_from_removal)
  not_checked_values %<>% remove_strings_with_length_less_than(min_length)
  not_checked_values %<>% sort_string_array()

  while(length(not_checked_values) >= 2) {
    current_value <- not_checked_values[[1]]
    similar_values <- agrep(
      current_value,
      tail(not_checked_values, -1),
      value = TRUE,
      max.distance = max_distance,
      ignore.case = ignore_case
    )

    if (length(similar_values)) {
      if (verbose) {
        print(paste0("Found similarities to word: ", current_value))
        print("Similar words are: ")
        print(similar_values)
        print(page_separator())
      }

      dataset %<>% replace_group_of_values_with_another_value(
        column,
        similar_values,
        current_value
      )
    }

    checked_values <- union(current_value, similar_values)
    not_checked_values %<>% setdiff(checked_values)
  }
  dataset
}


page_separator <- function(n=100) {
  strrep("-", n)
}

expands_months_between <- function(dataset, start_date_column, end_date_column, group_columns) {
  setDT(dataset)
  dataset[, month_difference := difference_in_months(get(start_date_column), get(end_date_column)) + 1]

  dataset <- dataset[, .SD[rep(1:.N, month_difference)]][, date:= seq(get(start_date_column), get(end_date_column), by = 'months'),
                                                         by = group_columns]

  dataset$month_difference = NULL
  return(dataset)
}

string_to_quosure <- function(string) {
  return (quo(!! sym(string)))
}

string_group_to_quosures <- function(group) {
  return (quos(!!! syms(group)))
}

# arg_to_quosure <- function(argument) {
#   enquo(argument)
# }

quosure_to_string <- function(quosure) {
  # quo_name(quosure)
  # rlang::quo_text()
  as.character(quosure)[2]
}

quosure_group_to_string <- function(group) {

}


exclude_values <- function(values, excluded_values) {
  return (setdiff(values, excluded_values))
}

# columns are string array
remove_NA_rows <- function(dataset, columns=NULL) {
  if (is.null(columns)) {
    columns <- colnames(dataset)
  }
  tmp <- complete.cases(dataset[, columns])
  return(dataset[tmp, ])
}

remove_empty_string_rows <- function(dataset, columns=NULL) {
  if (is.null(columns)) {
    columns <- colnames(dataset)
  }
  for (column in columns) {
    dataset %<>% remove_rows_with_value_in_a_group_of_values(column, c("", " "))
  }
  dataset
}

# column a string
# https://stackoverflow.com/questions/34616264/delete-rows-with-value-frequencies-lesser-than-x-in-r
remove_values_with_less_than_minimal_frequency <- function(dataset, column, minimal_frequency) {
  column %<>% string_to_quosure()
  dataset %>% group_by(!! column) %>% filter(n() > minimal_frequency) %>% as.data.frame()
}

remove_rows_with_value_in_a_group_of_values <- function(dataset, column, values_to_remove) {
  column_quo = string_to_quosure(column)
  dataset %>% filter(
    !(!!column_quo %in% values_to_remove)
  )
}

sending_datatable_decorator <- function(func, dataset, ...) {
  datatable_flag <- is.data.table(dataset)
  if (datatable_flag) {
    dataset %<>% as.data.frame()
  }

  dataset <- do.call(func, list(dataset, ...))

  if (datatable_flag) {
    dataset %<>% as.data.table()
  }
  return (dataset)
}


count_true_values <- function(array) {
  length(array[array==TRUE & !is.na(array)])
}

count_false_values <- function(array) {
  length(array[array==FALSE & !is.na(array)])
}

swap_columns_if <- function(dataset, condition, first_column, second_column, tmp_column = "tmp_column") {
  first_column_quo <- string_to_quosure(first_column)
  second_column_quo <- string_to_quosure(second_column)
  tmp_column_quo <- string_to_quosure(tmp_column)

  dataset %>%
    mutate(
      !! tmp_column := !! first_column_quo,
      !! first_column := if_else(condition(!! first_column_quo, !! second_column_quo), !! second_column_quo, !! first_column_quo),
      !! second_column := if_else(condition(!! tmp_column_quo, !! second_column_quo), !! tmp_column_quo, !! second_column_quo)
    ) %>%
    select(-!!tmp_column)
}

swap_dates_if_corrupt <- function(dataset, start_date, end_date, tmp_column = "tmp_column") {
  swap_columns_if(dataset, (function(x, y) {x > y}) ,start_date, end_date, tmp_column)
}

columns_classes <- function(dataset) {
  lapply(dataset, class)
}

rename_column <- function(dataset, current_name, new_name) {
  colnames(dataset)[colnames(dataset) == current_name] <- new_name
  dataset
}

read_all_worksheets <- function(path) {
  sheets <- getSheetNames(path)
  list_of_datasets <- lapply(sheets, read.xlsx, xlsxFile=path)
  ldply(list_of_datasets, data.frame)
}

lowercase_column <- function(dataset, column) {
  column_quo <- string_to_quosure(column)
  dataset %>% mutate(!! column := tolower(!! column_quo))
}

uppercase_column <- function(dataset, column) {
  column_quo <- string_to_quosure(column)
  dataset %>% mutate(!! column := toupper(!! column_quo))
}

read_xlsx_from_folder <- function(folder_path, ...) {
  read_files_from_folder(folder_path, read.xlsx, ...)
}

read_csv_from_folder <- function(folder_path, ...) {
  read_files_from_folder(folder_path, read.csv, ...)
}

read_files_from_folder <- function(folder_path, read_fuction,  ...) {
  files <- list.files(folder_path)
  files <- concatenate_paths(folder_path, files)
  list_of_datasets <- lapply(files, read_fuction, ...)
  rbind.fill(list_of_datasets)
}

sort_string_array <- function(string_array, decreasing=FALSE) {
  string_array[order(nchar(string_array), decreasing=decreasing)]
}

remove_strings_with_length_less_than <- function(string_array, min_length) {
  string_array[nchar(string_array) >= min_length]
}

extract_number <- function(string) {
  pattern="[[:digit:]]+"
  as.numeric(regmatches(string, regexpr(pattern, string)))
}

most_frequent_value <- function(...) {
  names(which.max(table(...)))
}

save_df_status <- function(dataset, name = NULL) {
  if (is.null(name)) {
    name <- deparse(substitute(dataset))
  }

  name <- paste0(name, "_df_status.Rdata")
  df_status_tmp <- df_status_v3(dataset, print_results=FALSE)
  save(df_status_tmp, file = concatenate_paths(data_exloration_path, name))
}

# If a column is a list then the following rules apply.
# Zero qunatity will represent number of lists of length 0 in that column.
# Empty string quantity will represent number of lists that contain empty strigs.
# NA quantity is the same.
# INF quantity cannot be computed so it is set to NA
df_status_v2 <- function(dataset, print_results=TRUE, max_char_length = 45, pretty_print=TRUE) {
  ## If input is NA then ask for a single vector. True if it is a single vector
  number_of_rows <- nrow(dataset)

  empty_str_func <- function(x) {
     if (is.list(x)) {
       return(sum(sapply(x, function(x) "" %in% x), na.rm = TRUE))
     }
    if (is.factor(x)) {
      x %<>% as.character()
    }
    ifelse(is.character(x), sum(x == "", na.rm = TRUE), 0)
  }

  if (is.null(max_char_length)) {
    column_names <- colnames(dataset)
  } else {
    column_names <- reduce_strings_length_to(colnames(dataset), max_char_length)
  }

  result <- data.frame(row.names = colnames(dataset)) %>%
    mutate(
      variable = column_names,
      q_0 = sapply(dataset, function(x) ifelse(is.list(x), sum(sapply(x, length) == 0, na.rm = TRUE), sum(x == 0, na.rm = TRUE))),
      p_0 = round(100 * q_0 / number_of_rows, 2),
      p_0 = if_else(p_0 == 100.00 & q_0 < number_of_rows, 99.99, p_0),
      q_na = sapply(dataset, function(x) sum(is.na(x))),
      p_na = round(100 * q_na / number_of_rows, 2),
      p_na = if_else(p_na == 100.00 & q_na < number_of_rows, 99.99, p_na),
      q_inf = sapply(dataset, function(x) ifelse(is.list(x), NA, sum(is.infinite(x)))),
      p_inf = round(100 * q_inf / number_of_rows, 2),
      p_inf = if_else(p_inf == 100.00 & q_inf < number_of_rows, 99.99, p_inf),
      q_es=sapply(dataset, empty_str_func),
      p_es = round(100 * q_es / number_of_rows, 2),
      p_es = if_else(p_es == 100.00 & q_es < number_of_rows, 99.99, p_es),
      class = sapply(dataset, class),
      unique = str_number(sapply(dataset, function(x) sum(!is.na(unique(x)))))
    )
  result %<>% mutate(
    q_0 = str_number(q_0),
    q_na = str_number(q_na),
    q_inf = str_number(q_inf),
    unique = str_number(unique),
    q_es = str_number(q_es)
  )

  max_variable <- max(nchar(column_names))
  filler <- c(paste(rep("-", max_variable), collapse = "") ,rep("----", length(colnames(result)) - 1))
  result[nrow(result) + 1, ] <- filler
  result[nrow(result) + 1, ] <- colnames(result)
  result[nrow(result) + 1, ] <- filler
  dim_row <- c("Dimension:", "", "Rows:", str_number(nrow(dataset)), "", "Columns:", str_number(ncol(dataset)))
  result[nrow(result) + 1, ] <- c(dim_row, rep("", length(colnames(result)) - length(dim_row)))

  ## Print or return results
  if(print_results) {
    if (pretty_print) {
      knitr::kable(result)
    } else {
      print(result)
    }
  } else {
    result
  }
}

df_status_v3 <- function(dataset, print_results=TRUE) {
  df_status_v2(dataset, print_results, max_char_length = NULL, pretty_print = FALSE)
}

str_number <- function(number) {
  format(number, big.mark = ",")
}

reduce_strings_length_to <- function(strings, max_length, add_dots=TRUE) {
  if (add_dots) {
    ifelse(nchar(strings) > max_length, paste0("...", substring(strings, nchar(strings) - max_length + 1)), strings)
  }else {
    ifelse(nchar(strings) > max_length, substring(strings, nchar(strings) - max_length + 1), strings)
  }
}

columns_whose_values_frequently_change <- function(dataset, column_to_group_by, column_to_order_by, min_frequency=0.6, verbose=FALSE) {
  group_by_quo = string_to_quosure(column_to_group_by)
  order_by_quo = string_to_quosure(column_to_order_by)

  percentage_df <- dataset %>%
    arrange(!! order_by_quo) %>%
    select(-!! order_by_quo) %>%
    group_by(!! group_by_quo) %>%
    summarise_all(function(x) count_false_values(x == lead(x)) / (length(x) - 1))

  mean_df <- percentage_df %>% select(-!! group_by_quo) %>% group_by() %>% summarise_all(mean)

  mean_vector <- unlist(mean_df[1, ])

  if (verbose) {
    print(sort(mean_vector, decreasing = TRUE))
  }

  names(mean_vector[mean_vector > min_frequency])
}


create_new_column_after_grouping_using_function_on_another_column <- function(dataset, group_by_columns, column_used, apply_function, suffix_for_new_column) {
  column_used_quo <- string_to_quosure(column_used)
  group_by_columns_quo <- string_group_to_quosures(group_by_columns)
  new_column_name <- paste0(column_used, suffix_for_new_column)

  dataset %>%
    group_by(!!! group_by_columns_quo) %>%
    mutate(!! new_column_name := apply_function(!! column_used_quo)) %>%
    ungroup() %>%
    as.data.frame()
}

create_new_multiple_columns_after_grouping_using_function_on_another_columns <- function(dataset, group_by_columns, columns_used, apply_function, suffix_for_new_columns) {
  for (col in columns_used) {
    dataset %<>% create_new_column_after_grouping_using_function_on_another_column(group_by_columns, col, apply_function, suffix_for_new_columns)
  }
  dataset
}

lag_columns_after_arranging_and_grouping <- function(dataset, time_column, group_columns, columns_to_lag, how_many_lags, suffix="_T-") {
  time_column_quo <- string_to_quosure(time_column)
  dataset %<>%
    arrange(!! time_column_quo)

  for (n_lag in 1:how_many_lags) {
    current_suffix = paste0(suffix, as.character(n_lag))
    dataset %<>% create_new_multiple_columns_after_grouping_using_function_on_another_columns(
      group_columns,
      columns_to_lag,
      (function(x) lag(x, n=n_lag)),
      current_suffix
    )
  }
  dataset
}

lag_percentage_columns_after_arranging_and_grouping <- function(dataset, time_column, group_columns, columns_to_lag, how_many_lags, suffix="_PERC_T-") {
  time_column_quo <- string_to_quosure(time_column)
  dataset %<>%
    arrange(!! time_column_quo)

  for (n_lag in 1:how_many_lags) {
    current_suffix = paste0(suffix, as.character(n_lag))

    tmp_func <- function(x) {
      difference <- x - lag(x, n=n_lag)
      divisor <- lag(x, n=n_lag)
      ifelse(divisor == 0, ifelse(difference > 0, 1, 0), difference / divisor)
    }

    dataset %<>% create_new_multiple_columns_after_grouping_using_function_on_another_columns(
      group_columns,
      columns_to_lag,
      tmp_func,
      current_suffix
    )
  }
  dataset
}

lag_difference_columns_after_arranging_and_grouping <- function(dataset, time_column, group_columns, columns_to_lag, how_many_lags, suffix="_DIFF_T-") {
  time_column_quo <- string_to_quosure(time_column)
  dataset %<>%
    arrange(!! time_column_quo)

  for (n_lag in 1:how_many_lags) {
    current_suffix = paste0(suffix, as.character(n_lag))

    dataset %<>% create_new_multiple_columns_after_grouping_using_function_on_another_columns(
      group_columns,
      columns_to_lag,
      (function(x) x - lag(x, n=n_lag)),
      current_suffix
    )
  }
  dataset
}

add_vectors <- function(...) {
  df <- data.frame(...)
  rowSums(df, na.rm=TRUE)
}

add_columns <- function(dataset, columns_to_add, new_column_name) {
  columns_to_add_quo <- string_group_to_quosures(columns_to_add)
  dataset %>% mutate(
    !! new_column_name := add_vectors(!!! columns_to_add_quo)
  )
}

add_columns_after_arranging_and_grouping <- function(dataset, time_column, group_by_columns, columns_to_combine, name_of_the_new_column) {
  time_column_quo <- string_to_quosure(time_column)
  group_by_columns_quo <- string_group_to_quosures(group_by_columns)
  columns_to_combine_quo <- string_group_to_quosures(columns_to_combine)

  dataset %>%
    arrange(!! time_column_quo) %>%
    group_by(!!! group_by_columns_quo) %>%
    mutate(
      !! name_of_the_new_column := add_vectors(!!! columns_to_combine_quo)
    ) %>%
    ungroup() %>%
    as.data.frame()
}


long_to_wide_v2 <- function(dataset, id_variable, time_variable, diff_columns = c(), percentage_columns = c(), time_separator="_") {
  dates <- unique(dataset[, time_variable])

  dataset <- dcast(
    dataset %>% as.data.table(),
    reformulate(response = id_variable, termlabels = time_variable),
    value.var = setdiff(colnames(dataset), c(id_variable, time_variable)),
    sep=time_separator
  )
  setDF(dataset)

  if (length(percentage_columns) > 0){
    percentage_func <- function(column, lagged_column) {
      difference <- column - lagged_column
      divisor <- lagged_column
      ifelse(divisor == 0, ifelse(difference > 0, 1, 0), difference / divisor)
    }
    dataset %<>% long_to_wide_v2_create_new_columns(dates, percentage_columns, percentage_func, "perc_diff_between", time_separator = time_separator)
  }
  if (length(diff_columns) > 0) {
    dataset %<>% long_to_wide_v2_create_new_columns(dates, diff_columns, (function(x, y) x - y), "diff_between", time_separator = time_separator)
  }

  dataset
}

long_to_wide_v2_create_new_columns <- function(dataset, dates, columns_to_apply, apply_function, sep_name, time_separator) {
  dates <- as.character(sort(dates, decreasing=TRUE))
  latest_date <- dates[[1]]

  for (column in columns_to_apply) {
    column_name <- paste(column, latest_date, sep=time_separator)
    column_name_quo <- string_to_quosure(column_name)

    for (i in 2:length(dates)) {
      current_date <- dates[i]
      current_column_name <- paste(column, current_date, sep=time_separator)
      current_column_name_quo <- string_to_quosure(current_column_name)
      dataset %<>% mutate(
        !! paste(column, sep_name, latest_date, "and", current_date, sep="_") := apply_function(!! column_name_quo, !! current_column_name_quo)
      )
    }
  }

  dataset
}


select_dates_with_month_offset <- function(dataset, date_column, date_start, month_offset) {
  date_end <- add_months(date_start, month_offset)
  date_column_quo <- string_to_quosure(date_column)
  dataset %>% filter(
    (!! date_column_quo >= date_start) & (!! date_column_quo <= date_end)
  )
}

# ... have to be lists
call_func_with_params <- function(func, ...) {
  do.call(func, c(...))
}

# https://stackoverflow.com/questions/3418128/how-to-convert-a-factor-to-integer-numeric-without-loss-of-information
factor_to_numeric <- function(factor) {
  # as.numeric(as.character(factor))
  as.numeric(levels(factor))[factor]
}

print_color <- function(color_func, ...) {
  cat(color_func(paste0(..., "\n")))
}

sample_v2 <- function(x, new_size) {
  if (length(x) == 1) {
    x <- 1:x
  }

  result <- c()
  x_length <- length(x)

  while (new_size > x_length) {
    new_size <- new_size - x_length
    result %<>% c(sample(x, x_length))
  }
  result %<>% c(sample(x, new_size))

  result
}

vector_to_string <- function(vector, sep = ", ") {
  paste(vector, collapse = sep)
}

list_to_string <- function(list, sep = ", ") {
  combined <- c()
  list_names <- names(list)
  for (i in 1:length(list)) {
    combined %<>% c(paste(list_names[i], list[i], sep=": "))
  }
  paste(combined, collapse = sep)
}

dates_to_relative_dates_using_month_difference <- function(dates, relative_date=NULL, string_relative_date= "T", sep="") {
  if (is.null(relative_date)) {
    relative_date = max(dates)
  }
  paste(string_relative_date, -difference_in_months(dates, relative_date), sep=sep)
}

vector_to_binary_vector <- function(vector, threshold=0.5) {
  ifelse(vector <= threshold, 0, 1)
}


remove_pattern <- function(string, pattern) {
  str_replace(string, pattern, "")
}

select_columns_with_pattern <- function(dataset, pattern) {
  selected_columns <- str_subset(colnames(dataset), pattern)
  dataset %>% keep_columns(selected_columns)
}

select_columns_with_patterns <- function(dataset, patterns) {
  pattern <- paste(patterns, collapse = "|")
  select_columns_with_pattern(dataset, pattern)
}

print_on_the_same_line <- function(...) {
  cat("\r", ..., sep = "")
}

corr_with_column <- function(dataset, column) {
  result <- as.data.frame(cor(dataset))[column]

  # Stupid hack to remove row by name without losing the dataframe shape.
  result$column <- rownames(result)
  result <- result[!rownames(result) == column, ]
  # print(result)
  column_quo <- string_to_quosure(column)
  result %<>% arrange(desc(!! column_quo))

  rownames(result) <- result$column
  result$column <- NULL
  result
}


compare_datasets <- function(dataset1, dataset2, show_df_status = FALSE) {
  name1 <- deparse(substitute(dataset1))
  name2 <- deparse(substitute(dataset2))
  print_color(
    blue,
    "First dataset: ", name1, "\n",
    "Second dataset: ", name2
  )

  dim1 <- dim(dataset1)
  dim2 <- dim(dataset2)
  if (all(dim1 == dim2)) {
    print_color(
      green,
      "Datasets have the same number of rows and columns. ", "Which is ",
      ncol(dataset1), " columns and ", nrow(dataset1), " rows."
    )
  } else {
    print_color(
      red,
      "Datasets do not have the same number of rows and columns."
    )
    print_color(
      cyan,
      "First dataset has ", ncol(dataset1), " columns and ", nrow(dataset1), " rows.", "\n",
      "Second dataset has ", ncol(dataset2), " columns and ", nrow(dataset2), " rows."
    )
  }

  colnames1 <- colnames(dataset1)
  colnames2 <- colnames(dataset2)
  if (equal_sets(colnames1, colnames2)) {
    print_color(
      green,
      "Datasets have the columns of the same name."
    )
  } else {
    print_color(
      red,
      "Datasets do not have columns of the same name."
    )
    if (length(setdiff(colnames1, colnames2))) {
      print_color(
        cyan,
        "First dataset has these columns not found in second dataset: \n\t", vector_to_string(setdiff(colnames1, colnames2), sep="\n\t")
      )
    }
    if (length(setdiff(colnames2, colnames1))) {
      print_color(
        cyan,
        "Second dataset has these columns not found in first dataset: \n\t", vector_to_string(setdiff(colnames2, colnames1), sep="\n\t")
      )
    }

  }

  if (show_df_status) {
    print_color(yellow, page_separator())
    print_color(magenta, "First dataset df_status:")
    df_status_v3(dataset1)

    print_color(yellow, page_separator())
    print_color(magenta, "Second dataset df_status:")
    df_status_v3(dataset2)
  }
}

equal_sets <- function(set1, set2) {
  length(setdiff(set1, set2)) == 0 && length(setdiff(set2, set1)) == 0
}

print_columns_with_unique_values_less_than <- function(dataset, threshold = 6) {
  for (c in colnames(dataset)){
    if (dataset[[c]] %>% unique() %>% length() < threshold) {
      print_color(
        green,
        c
      )
      print_color(
        blue,
        dataset[[c]] %>% table() %>% list_to_string(sep = ",\n")
      )
      print_color(
        yellow,
        page_separator()
      )
    }
  }
}

