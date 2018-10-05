print_func <- function(monthly_connections, dataset) {
  print(paste0("Done with ", deparse(substitute(dataset))))
  print(str_number(dim(monthly_connections)))
  print(page_separator())
}


data_for_time_period <- function(churn_dataset, date_start, month_offset, id_column, date_column, target_column, diff_collumns, percentage_diff_columns) {
  data <- churn_dataset %>%
    remove_columns(target_column) %>%
    select_dates_with_month_offset(
      date_column,
      date_start,
      month_offset
    )

  date_column_quo <- string_to_quosure(date_column)

  data %<>%
    mutate(
      !!date_column  := dates_to_relative_dates_using_month_difference(!! date_column_quo)
    ) %>%
    long_to_wide_v2(
      id_variable = id_column,
      time_variable = date_column,
      diff_columns = diff_columns,
      percentage_columns = percentage_diff_columns
    )
  data %<>% remove_NA_rows()

  target_date <- add_months(date_start, month_offset + 1)
  labels <- churn_dataset %>%
    filter(!!date_column_quo == target_date) %>%
    keep_columns(id_column, target_column)

  combined <- merge(
    data,
    labels,
    by=id_column
  )

  data <- combined %>% remove_columns(id_column, target_column)
  labels <- combined[[target_column]]

  list(
    data = data,
    labels = labels
  )
}

set_to_xgb_dmatrix <- function(set) {
  xgb.DMatrix(data = as.matrix(set$data), label = set$labels)
}

# minority increase has to be 2 or more
SMOTE_v2 <- function(X, Y, minority_increase = 2, minority_percentage = 0.5, k = 5) {
  y_is_factor = is.factor(Y)
  if (!y_is_factor) {
    Y %<>% as.factor()
  }

  smoted_data <- ubSMOTE(
    X = X,
    Y = Y,
    perc.over = (minority_increase - 1) * 100,
    k = k,
    perc.under = 0
  )

  new_minority_size <- length(smoted_data$Y)
  total_size <- new_minority_size / minority_percentage
  new_majority_size <- total_size - new_minority_size

  Y %<>% factor_to_numeric()
  majority_indexes <- which(Y == 0)
  new_majority_indexes <- sample_v2(
    majority_indexes,
    new_majority_size
  )

  smoted_data$X %<>% rbind(X[new_majority_indexes, ])
  smoted_data$Y %<>% factor_to_numeric()
  smoted_data$Y %<>% c(Y[new_majority_indexes])

  rownames(smoted_data$X) <- 1:nrow(smoted_data$X)
  shuffle_indexes <- sample(nrow(smoted_data$X))
  smoted_data$X <- smoted_data$X[shuffle_indexes, ]
  smoted_data$Y <- smoted_data$Y[shuffle_indexes]

  if (y_is_factor) {
    smoted_data$Y %<>% as.factor()
  }

  smoted_data
}


smote_optimizer <- function(data, labels, parameter_ranges, xgb_parameters, fold_times, fold_seed = 555, xgboost_seed = 556, smote_seed = 557, return_combinations_dataframe = FALSE) {
  smote_train_func <- function(training_data, training_labels, validation_data, validation_labels, combination) {
    set.seed(smote_seed)
    smoted <- call_func_with_params(
      ubSMOTE,
      list(X = training_data, Y = as.factor(training_labels)),
      combination
    )

    training_data <- smoted$X
    training_labels <- factor_to_numeric(smoted$Y)

    training_dmatrix <- xgb.DMatrix(as.matrix(training_data), label = training_labels)
    validation_dmatrix <- xgb.DMatrix(as.matrix(validation_data), label = validation_labels)

    set.seed(xgboost_seed)
    xgboost_model <- call_func_with_params(
      xgb.train,
      list(
        data = training_dmatrix,
        watchlist = list(training = training_dmatrix, validation = validation_dmatrix),
        base_score = mean(training_labels)
      ),
      xgb_parameters
    )

    xgboost_model$best_score
  }

  abstract_optimizer(
    data,
    labels,
    parameter_ranges = parameter_ranges,
    train_func = smote_train_func,
    fold_times = fold_times,
    fold_seed = fold_seed,
    return_combinations_dataframe = return_combinations_dataframe
  )
}


xgboost_optimizer <- function(data, labels, parameter_ranges, fold_times, xgboost_params, fold_seed = 555, xgboost_seed = 556, return_combinations_dataframe = FALSE) {
  xgboost_train_func <- function(training_data, training_labels, validation_data, validation_labels, combination) {
    training_dmatrix <- xgb.DMatrix(as.matrix(training_data), label = training_labels)
    validation_dmatrix <- xgb.DMatrix(as.matrix(validation_data), label = validation_labels)

    set.seed(xgboost_seed)
    xgboost_model <- call_func_with_params(
      func = xgb.train,
      list(
        params = combination,
        data = training_dmatrix,
        watchlist = list(training = training_dmatrix, validation = validation_dmatrix),
        base_score = mean(training_labels)
      ),
      xgboost_params
    )

    xgboost_model$best_score
  }

  result <- abstract_optimizer(
    data,
    labels,
    parameter_ranges = parameter_ranges,
    train_func = xgboost_train_func,
    fold_times = fold_times,
    fold_seed = fold_seed,
    return_combinations_dataframe = return_combinations_dataframe
  )

  if (return_combinations_dataframe) {
    result$xgboost_params <- xgboost_params
  }

  result
}

# train func takes training_data, training_labels, validation_data, validation_labels
# and params, it returns score
abstract_optimizer <- function(data, labels, parameter_ranges, train_func, fold_times, fold_seed = 555, return_combinations_dataframe = FALSE) {
  set.seed(fold_seed)
  folds <- createFolds(
    y = labels,
    k = fold_times
  )

  score_sum_vector <- c()
  best_combination <- NULL
  best_score_sum <- NULL

  parameter_combinations_df <- expand.grid(parameter_ranges)
  number_of_combinations <- nrow(parameter_combinations_df)
  print_color(
    red,
    "There are ",
    number_of_combinations,
    " possible combination."
  )
  print_color(
    yellow,
    page_separator()
  )

  for (i in 1:number_of_combinations) {
    score_sum <- 0
    combination <- as.list(unlist(parameter_combinations_df[i, ]))
    print_color(
      cyan,
      "Combination ",
      i,
      " out of ",
      number_of_combinations,
      "."
    )
    print_color(
      blue,
      "Parameters combination: ",
      "\n\t",
      list_to_string(combination, "\n\t")
    )

    for (fold_index in 1:length(folds))  {
      fold_data <- folds[[fold_index]]

      training_data <- data[-fold_data, ]
      training_labels <- labels[-fold_data]

      validation_data <- data[fold_data, ]
      validation_labels <- labels[fold_data]

      score <- train_func(
        training_data,
        training_labels,
        validation_data,
        validation_labels,
        combination
      )

      score_sum <- score_sum + score
    }
    print_color(
      cyan,
      "Score sum after ",
      fold_times,
      " folds is ",
      score_sum,
      "."
    )
    print_color(
      cyan,
      "Average score is ",
      score_sum / fold_times,
      "."
    )
    score_sum_vector %<>% append(score_sum)

    if (is.null(best_score_sum) || score_sum > best_score_sum) {
      print_color(
        green,
        "Found the new best combination. ",
        "Best score sum before was ", best_score_sum,
        ", while new one is ", score_sum,
        "."
      )
      best_score_sum <- score_sum
      best_combination <- combination
    }

    print_color(
      yellow,
      page_separator()
    )
  }

  parameter_combinations_df$score_sum <- score_sum_vector
  parameter_combinations_df$average_score <- score_sum_vector / fold_times
  if (return_combinations_dataframe) {
    return (list(
      combinations = parameter_combinations_df,
      best_combination = best_combination,
      parameter_ranges = parameter_ranges,
      fold_times = fold_times
    ))
  }

  best_combination
}

# compute score first takes actual label and then predicted so it is function(actual, prediction)
optimize_prediction_threshold <- function(prediction, actual, compute_score, threshold_values = seq(0, 1, 0.01), verbose = FALSE) {
  best_score <- NULL
  best_threshold <- NULL

  if (verbose) {
    print_color(
      red,
      "There is a total of ",
      length(threshold_values),
      " different combinations."
    )
    print_color(
      yellow,
      page_separator()
    )
  }

  for (threshold in threshold_values) {
    if (verbose) {
      print_color(
        blue,
        "Testing ",
        threshold,
        "."
      )
    }

    prediction_binary <- ifelse(prediction <= threshold, 0, 1)
    score <- compute_score(actual, prediction_binary)

    if (verbose) {
      message("Got score: ", round(score, 5), ", for threshold ", threshold)
    }

    if (is.null(best_score) || score > best_score) {
      if (verbose) {
        print_color(
          green,
          "This is the new best score."
        )
        print_color(
          green,
          "Last one was ", ifelse(is.null(best_score), "NULL", round(best_score, 5)), ". ",
          "New one is ", round(score, 5), "."
        )
      }

      best_score <- score
      best_threshold <- threshold
    }

    if(verbose) {
      print_color(
        yellow,
        page_separator()
      )
    }
  }

  best_threshold
}

optimize_prediction_threshold_auc <- function(prediction, actual, threshold_values = seq(0, 1, 0.01), verbose = FALSE) {
  optimize_prediction_threshold(prediction, actual, compute_score = auc, threshold_values = threshold_values, verbose=verbose)
}

optimize_prediction_threshold_f1_score <- function(prediction, actual, threshold_values = seq(0, 1, 0.01), verbose = FALSE) {
  optimize_prediction_threshold(prediction, actual, compute_score = f1_score, threshold_values = threshold_values, verbose=verbose)
}

optimize_prediction_threshold_balanced_accuracy <- function(prediction, actual, threshold_values = seq(0, 1, 0.01), verbose = FALSE) {
  compute_balance_accuracy <- function(actual, predicted) {
    if (!is.factor(actual)) {
      actual %<>% as.factor()
    }
    if (!is.factor(predicted)) {
      predicted %<>% as.factor()
    }

    cm <- confusionMatrix(
      data = predicted,
      reference = actual,
      positive = "1"
    )
    as.numeric(cm$byClass["Balanced Accuracy"])
  }

  optimize_prediction_threshold(prediction, actual, compute_score = compute_balance_accuracy, threshold_values = threshold_values, verbose=verbose)
}

optimize_prediction_threshold_kappa <- function(prediction, actual, threshold_values = seq(0, 1, 0.01), verbose = FALSE) {
  compute_balance_accuracy <- function(actual, predicted) {
    if (!is.factor(actual)) {
      actual %<>% as.factor()
    }
    if (!is.factor(predicted)) {
      predicted %<>% as.factor()
    }

    cm <- confusionMatrix(
      data = predicted,
      reference = actual,
      positive = "1"
    )
    as.numeric(cm$overall["Kappa"])
  }

  optimize_prediction_threshold(prediction, actual, compute_score = compute_balance_accuracy, threshold_values = threshold_values, verbose=verbose)
}

optimization_predction_threshold_results <- function(prediction, actual, threshold_values = seq(0, 1, 0.01), verbose = FALSE) {
  results <- list(
    threshold = c(),
    true_positive = c(),
    true_negative = c(),
    false_positive = c(),
    false_negative = c(),
    balanced_accuracy = c(),
    accuracy = c(),
    f1_score = c(),
    precision = c(),
    recall = c(),
    kappa = c(),
    auc = c()
  )
  threshold_values_length <- length(threshold_values)

  for (i in 1:threshold_values_length) {
    if (verbose) {
      print_color(
        green,
        "Computing iteration ",
        i,
        "/",
        threshold_values_length,
        "."
      )
    }

    threshold <- threshold_values[[i]]
    results$threshold %<>% append(threshold)
    prediction_binary <- vector_to_binary_vector(prediction, threshold)

    cm <- confusionMatrix(
      data = as.factor(prediction_binary),
      reference = as.factor(actual),
      positive = "1"
    )

    results$true_positive %<>% append(cm$table[2, 2])
    results$true_negative %<>% append(cm$table[1, 1])
    results$false_positive %<>% append(cm$table[2, 1])
    results$false_negative %<>% append(cm$table[1, 2])
    results$accuracy %<>% append(cm$overall["Accuracy"])
    results$kappa %<>% append(cm$overall["Kappa"])
    results$balanced_accuracy %<>% append(cm$byClass["Balanced Accuracy"])
    results$f1_score %<>% append(cm$byClass["F1"])
    results$recall %<>% append(cm$byClass["Recall"])
    results$precision %<>% append(cm$byClass["Precision"])

    results$auc %<>% append(auc(actual, prediction_binary))
  }

  as.data.frame(results)
}

f1_score <- function(actual, prediction, positive = "1") {
  if (!is.factor(actual)) {
    actual %<>% as.factor()
  }
  if (!is.factor(prediction)) {
    prediction %<>% as.factor()
  }

  precision <- posPredValue(prediction, actual, positive = positive)
  recall <- sensitivity(prediction, actual, positive = positive)

  if (is.na(precision) || is.na(recall)) {
    return (0.0)
  }

  (2 * precision * recall) / (precision + recall)
}

xgb_importance_to_original <- function(importance, regex = "_T-?[:digit:]+$") {
  importance %>%
    mutate(Feature = remove_pattern(Feature, regex)) %>%
    group_by(Feature) %>%
    summarise_all(sum)
}

xgb_importance_to_original_dt <- function(importance, regex = "_T-?[:digit:]+$") {
  sending_datatable_decorator(xgb_importance_to_original, importance, regex = regex)
}


xgb_feature_importance_results_best_n <- function(
  best_n_features_values,
  training_set,
  validation_set,
  model,
  smote_params,
  xgboost_constant_params,
  xgboost_train_params = NULL,
  verbose = FALSE
) {
  importance <- xgb_original_importance(model)

  if (is.null(best_n_features_values)) {
    best_n_features_values = 1:nrow(importance)
  }

  gain_threshold_values <- importance[best_n_features_values, ][["Gain"]]
  xgb_feature_importance_results(gain_threshold_values, training_set, validation_set, model, smote_params, xgboost_constant_params, xgboost_train_params, verbose)
}

xgb_feature_importance_results <- function(
  gain_threshold_values,
  training_set,
  validation_set,
  model,
  smote_params,
  xgboost_constant_params,
  xgboost_train_params = NULL,
  verbose = FALSE
) {
  if (is.null(xgboost_train_params)) {
    xgboost_train_params <- model$params
  }

  importance <- xgb_original_importance(model)
  results <- list(
    id = c(),
    gain_threshold = c(),
    number_of_features = c(),
    score = c()
  )
  features = list(
    id = c(),
    features = list()
  )

  number_of_gain_threshold_values <- length(gain_threshold_values)
  for (i in 1:number_of_gain_threshold_values) {
    if (verbose) {
      print_color(
        blue,
        "Computing ",
        i,
        "/",
        number_of_gain_threshold_values,
        "."
      )
    }

    gain_threshold <- gain_threshold_values[i]
    current_features <- importance[Gain >= gain_threshold]$Feature

    training_set_tmp <- training_set
    training_set_tmp$data %<>% select_columns_with_patterns(current_features)
    validation_set_tmp <- validation_set
    validation_set_tmp$data %<>% select_columns_with_patterns(current_features)

    xgb_model <- tomek_smote_xgboost(
      training_set = training_set_tmp,
      validation_set = validation_set_tmp,
      smote_params = smote_params,
      xgboost_train_params = xgboost_train_params,
      xgboost_constant_params = xgboost_constant_params
    )

    results$id %<>% append(i)
    results$gain_threshold %<>% append(gain_threshold)
    results$number_of_features %<>% append(length(current_features))
    results$score %<>% append(xgb_model$best_score)

    features$id %<>% append(i)
    features$features[[i]] <- current_features

    rm(training_set_tmp)
    rm(validation_set_tmp)
    gc()
  }

  list(
    results = as.data.frame(results),
    features = features
  )
}

# Without the T-1, T0....
xgb_original_importance <- function(model) {
  importance <- xgb.importance(model = model)
  importance %>%
    xgb_importance_to_original() %>%
    arrange(desc(Gain)) %>%
    as.data.table()
}

tomek_smote_xgboost <- function(training_set,
                                validation_set,
                                smote_params,
                                xgboost_train_params,
                                xgboost_constant_params) {
  tomek_result <- ubTomek(training_set$data, as.factor(training_set$labels), verbose = FALSE)
  training_set$data <- tomek_result$X
  training_set$labels <- factor_to_numeric(tomek_result$Y)

  smote_result <- call_func_with_params(
    ubSMOTE,
    list(X = training_set$data, Y = as.factor(training_set$labels)),
    smote_params
  )
  training_set$data <- smote_result$X
  training_set$labels <- factor_to_numeric(smote_result$Y)

  training_dmatrix <- set_to_xgb_dmatrix(training_set)
  validation_dmatrix <- set_to_xgb_dmatrix(validation_set)

  xgb_model <- call_func_with_params(
    xgb.train,
    list(
      params = xgboost_train_params,
      data = training_dmatrix,
      watchlist = list(training = training_dmatrix, validation = validation_dmatrix)
      # base_score = mean(training_set$labels)
    ),
    xgboost_constant_params
  )

  xgb_model
}
