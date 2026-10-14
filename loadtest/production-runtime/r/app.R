library(shiny)
library(jsonlite)
version <- readLines("version.txt", warn=FALSE)[1]
data_dir <- Sys.getenv("SHINYHUB_APP_DATA")
if (file.exists("fail-startup")) stop("deliberate candidate startup failure")
writeLines("starting", file.path(data_dir, paste0("starting-", version)))
while (file.exists(file.path(data_dir, paste0("gate-", version)))) Sys.sleep(0.05)
Sys.sleep(1)
dataset <- readBin(file.path(data_dir, "dataset.bin"), "double", n=2000000L, size=8L, endian="little")
stopifnot(length(dataset) == 2000000L)
snapshot <- if (file.exists(file.path(data_dir, "producer.txt"))) paste(readLines(file.path(data_dir, "producer.txt"), warn=FALSE), collapse="\n") else "initial"
ui <- fluidPage(numericInput("multiplier", "Applied filter", 2, min=1, max=10), actionButton("tick", "Compute"), verbatimTextOutput("result"))
server <- function(input, output, session) {
  private <- raw(262144L)
  private[seq(1L, length(private), by=4096L)] <- as.raw(1)
  output$result <- renderText(as.character(toJSON(list(session=session$token, tick=input$tick,
    multiplier=input$multiplier, calculation=sum(dataset[1:100000]) * input$multiplier,
    dataset_checksum=sum(dataset), dataset_bytes=length(dataset) * 8L, memory_bytes=length(private),
    version=version, data_snapshot=snapshot, process_pid=Sys.getpid()), auto_unbox=TRUE, digits=16)))
}
shinyApp(ui, server)
