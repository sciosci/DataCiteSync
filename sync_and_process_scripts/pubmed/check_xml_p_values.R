# =============================================================================
# check_pvalues.R
#
# Read a .nxml file, extract all text containing NHST results,
# and run statcheck to flag any mismatches between reported p-values
# and recomputed p-values.
#
# Usage:
#   Rscript check_pvalues.R /path/to/folder
# =============================================================================

#options(repos = c(CRAN = "https://cran.r-project.org/"))
#install.packages("pryr")
library(fs)
library(xml2)      # for parsing .nxml
library(statcheck) # for checking p-values
library(jsonlite)
library(pryr)      # for mem_used()



# Function to print memory usage
print_memory <- function(message = "") {
  mem <- pryr::mem_used()
  cat(message, "Memory used: ", format(mem / 1024^2, digits = 2), " MB\n", sep = "")
}

# Function to limit memory usage (returns TRUE if safe to proceed)
check_memory_limit <- function(limit_mb = 4000) {
  mem <- pryr::mem_used() / 1024^2  # Convert to MB
  if (mem > limit_mb) {
    cat("WARNING: Memory usage (", format(mem, digits = 2), " MB) exceeds limit (", limit_mb, " MB).\n", sep = "")
    cat("Forcing garbage collection...\n")
    gc(verbose = FALSE, full = TRUE)
    mem_after <- pryr::mem_used() / 1024^2
    cat("Memory after GC: ", format(mem_after, digits = 2), " MB\n", sep = "")
    return(mem_after < limit_mb)
  }
  return(TRUE)
}

extract_text_from_nxml <- function(file_path) {
  tryCatch({
    doc <- read_xml(file_path)
    nodes <- xml_find_all(doc, ".//p")
    text <- paste(xml_text(nodes), collapse = " ")
    # Clean up to free memory
    rm(doc, nodes)
    gc(verbose = FALSE)
    return(text)
  }, error = function(e) {
    cat("ERROR processing file:", file_path, "-", e$message, "\n")
    return(NULL)
  })
}

process_one_file <- function(path) {
  tryCatch({
    # Check if we should continue based on memory usage
    if (!check_memory_limit(4000)) {  # 4GB limit
      cat("Memory limit reached. Skipping file:", basename(path), "\n")
      return(NULL)
    }

    cat("Processing:", basename(path), "\n")

    # Process the file
    txt <- extract_text_from_nxml(path)
    if (is.null(txt)) return(NULL)

    res <- statcheck(text = txt)

    # Free memory immediately
    rm(txt)
    gc(verbose = FALSE)

    if (!is.null(res) && nrow(res) > 0) {
      errors_found <- res$error
      if (any(errors_found)) {
        cat("Found", sum(errors_found), "errors in", basename(path), "\n")
        error_results <- res$raw[errors_found]
        rm(res)
        gc(verbose = FALSE)
        return(error_results)
      }
    }

    # Clean up
    rm(res)
    gc(verbose = FALSE)
    return(NULL)
  }, error = function(e) {
    cat("ERROR in process_one_file:", e$message, "\n")
    return(NULL)
  })
}

# Process files in batches to avoid memory issues
process_files_in_batches <- function(file_paths, batch_size = 50) {
  total_files <- length(file_paths)
  cat("Processing", total_files, "files in batches of", batch_size, "\n")

  all_errors <- list()

  for (i in seq(1, total_files, by = batch_size)) {
    print_memory("Before batch: ")
    batch_end <- min(i + batch_size - 1, total_files)

    cat("\n=== Processing batch", ceiling(i/batch_size), "of", ceiling(total_files/batch_size),
        "(files", i, "to", batch_end, ") ===\n")

    batch_files <- file_paths[i:batch_end]
    batch_results <- lapply(batch_files, process_one_file)
    names(batch_results) <- basename(batch_files)

    # Keep only non-NULL results
    batch_errors <- batch_results[!sapply(batch_results, is.null)]

    # Add to the main results list
    all_errors <- c(all_errors, batch_errors)

    # Write intermediate results to avoid losing progress
    if (length(batch_errors) > 0) {
      temp_filename <- paste0("statistical_errors_batch_", ceiling(i/batch_size), ".json")
      write_json(batch_errors, path = temp_filename, pretty = TRUE, auto_unbox = TRUE)
      cat("Wrote", length(batch_errors), "files with errors to", temp_filename, "\n")
    }

    # Clean up batch results to free memory
    rm(batch_results, batch_errors, batch_files)
    gc(verbose = TRUE, full = TRUE)
    print_memory("After batch: ")
  }

  return(all_errors)
}

main <- function() {
  # Install pryr if not already installed
  if (!requireNamespace("pryr", quietly = TRUE)) {
    install.packages("pryr")
    library(pryr)
  }

  # Print initial memory state
  print_memory("Initial ")

  # Process in smaller directory chunks to avoid loading too many file paths at once
  input_dir <- <path_to_extracted_xml_file_repository>

  # Get all subdirectories to process them one by one
  subdirs <- dir_ls(input_dir, type = "directory", recurse = FALSE)

  if (length(subdirs) == 0) {
    # If no subdirectories, process files directly
    nxmls <- dir_ls(input_dir, recurse = TRUE, glob = "*.nxml")
    all_errors <- process_files_in_batches(nxmls)
  } else {
    # Process each subdirectory separately
    all_errors <- list()
    for (subdir in subdirs) {
      cat("\n=== Processing directory:", basename(subdir), "===\n")

      # Get files in this subdirectory
      nxmls <- dir_ls(subdir, recurse = TRUE, glob = "*.nxml")
      cat("Found", length(nxmls), "NXML files in", basename(subdir), "\n")

      # Skip empty directories
      if (length(nxmls) == 0) {
        cat("No NXML files found in", basename(subdir), "- skipping\n")
        next
      }

      # Process this batch of files
      subdir_errors <- process_files_in_batches(nxmls)

      # Write subdirectory results
      if (length(subdir_errors) > 0) {
        subdir_filename <- paste0("errors_", basename(subdir), ".json")
        write_json(subdir_errors, path = subdir_filename, pretty = TRUE, auto_unbox = TRUE)
        cat("Wrote", length(subdir_errors), "files with errors to", subdir_filename, "\n")
      }

      # Add to overall results
      all_errors <- c(all_errors, subdir_errors)

      # Clean up
      rm(nxmls, subdir_errors)
      gc(verbose = TRUE, full = TRUE)
      print_memory("After directory: ")
    }
  }

  # Final results
  cat("\n====== FINAL SUMMARY ======\n")
  cat("Found errors in", length(all_errors), "files\n")

  if (length(all_errors) > 0) {
    # Show the first few errors as examples
    cat("\nSample errors found:\n")
    for (i in seq_along(all_errors)[1:min(3, length(all_errors))]) {
      cat("In file", names(all_errors)[i], ":\n")
      print(all_errors[[i]])
      cat("\n")
    }

    # Write final results to JSON
    write_json(all_errors,
               path       = "all_statistical_errors.json",
               pretty     = TRUE,
               auto_unbox = TRUE)

    message("Wrote ", length(all_errors), " files with statistical errors to all_statistical_errors.json")
  } else {
    cat("No statistical errors found in any files.\n")
  }

  # Final memory usage
  print_memory("Final ")
}

# Run the main function
main()
