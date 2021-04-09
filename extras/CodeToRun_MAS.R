library(PLECompetingRisk)

# Optional: specify where the temporary files (used by the Andromeda package) will be created:
options(andromedaTempFolder = "D:/andromedaTemp")

# Maximum number of cores to be used:
maxCores <- 1

# The folder where the study intermediate and result files will be written:
outputFolder <- "D:/PLECompetingRisk"

# Details for connecting to the server:
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "pdw",
                                                                server = keyring::key_get("pdwServer"),
                                                                user = NULL,
                                                                password = NULL,
                                                                port =  keyring::key_get("pdwPort"))

cdmDatabaseSchema <- "cdm_optum_extended_dod_v1194.dbo"
cohortDatabaseSchema <- "scratch.dbo"
databaseId<- "OptumDOD"
databaseName <- "Optum’s Clinformatics® Extended Data Mart"
databaseDescription <- "Optum Clinformatics Extended DataMart is an adjudicated US administrative health claims database for members of private health insurance, who are fully insured in commercial plans or in administrative services only (ASOs), Legacy Medicare Choice Lives (prior to January 2006), and Medicare Advantage (Medicare Advantage Prescription Drug coverage starting January 2006). The population is primarily representative of commercial claims patients (0-65 years old) with some Medicare (65+ years old) however ages are capped at 90 years. It includes data captured from administrative claims processed from inpatient and outpatient medical services and prescriptions as dispensed, as well as results for outpatient lab tests processed by large national lab vendors who participate in data exchange with Optum. This dataset also provides date of death (month and year only) for members with both medical and pharmacy coverage from the Social Security Death Master File (however after 2011 reporting frequency changed due to changes in reporting requirements) and location information for patients is at the US state level."
cohortTable <- "msuchard_cr"
outputFolder <- "d:/PLECompetingRisk_optum"
oracleTempSchema <- NULL

execute(connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        oracleTempSchema = oracleTempSchema,
        outputFolder = outputFolder,
        databaseId = databaseId,
        databaseName = databaseName,
        databaseDescription = databaseDescription,
        createCohorts = FALSE,
        synthesizePositiveControls = FALSE,
        runAnalyses = FALSE,
        packageResults = FALSE,
        maxCores = maxCores)

# resultsZipFile <- file.path(outputFolder, "export", paste0("Results_", databaseId, ".zip"))
# dataFolder <- file.path(outputFolder, "shinyData")
# 
# # You can inspect the results if you want:
# prepareForEvidenceExplorer(resultsZipFile = resultsZipFile, dataFolder = dataFolder)
# launchEvidenceExplorer(dataFolder = dataFolder, blind = TRUE, launch.browser = FALSE)
# 
# # Upload the results to the OHDSI SFTP server:
# privateKeyFileName <- ""
# userName <- ""
# uploadResults(outputFolder, privateKeyFileName, userName)

# Run competing risk analyses
library(dplyr)

riskId <- 317

cmAnalysisListFile <- system.file("settings",
                                  "cmAnalysisList.json",
                                  package = "PLECompetingRisk")
cmAnalysisList <- CohortMethod::loadCmAnalysisList(cmAnalysisListFile)

replacePopName <- function(studyPopFile, sharedPsFile, riskId) {
        if (studyPopFile != "") {
                fileName <- sub("o\\d+\\.", paste0("o", riskId, "."), studyPopFile)
        } else {
                fileName <- sub("\\.rds", paste0("_o", riskId, ".rds"), sharedPsFile)
                fileName <- sub("Ps", "StudyPop", fileName)
                fileName <- sub("p\\d+_", "", fileName)
        }
        return(fileName)
}

replaceOutcomeName <- function(fileName, riskId) {
        sub("\\.rds", paste0("_r", riskId, ".rds"), fileName)
}

savedOutputFolder <- outputFolder
outputFolder <- paste0(outputFolder, "/cmOutput")


omf <- readRDS(file.path(savedOutputFolder, "cmOutput/outcomeModelReference.rds")) %>% 
        filter(riskPopFile == "") %>%
        filter(outcomeId != riskId) %>% 
        rowwise() %>%
        mutate(riskId = riskId,
               riskPopFile = replacePopName(studyPopFile, sharedPsFile, riskId), # Should always be loaded with the StudyPop_o###.rds file name
               outcomeModelFile = replaceOutcomeName(outcomeModelFile, riskId))


# Copied from inside CohortMethod::RunAnalysis.R
createOutcomeModelTask <- function(i) {
        refRow <- subset[i, ]
        analysisRow <- ParallelLogger::matchInList(cmAnalysisList,
                                                   list(analysisId = refRow$analysisId))[[1]]
        args <- analysisRow$fitOutcomeModelArgs
        args$control$threads <- 1
        if (refRow$strataFile != "") {
                studyPopFile <- refRow$strataFile
        } else if (refRow$psFile != "") {
                studyPopFile <- refRow$psFile
        } else {
                studyPopFile <- refRow$studyPopFile
        }
        
        prefilteredCovariatesFile <- refRow$prefilteredCovariatesFile
        if (prefilteredCovariatesFile != "") {
                prefilteredCovariatesFile = file.path(outputFolder, refRow$prefilteredCovariatesFile)
        }
        return(list(cohortMethodDataFile = file.path(outputFolder, refRow$cohortMethodDataFile),
                    prefilteredCovariatesFile = prefilteredCovariatesFile,
                    args = args,
                    studyPopFile = file.path(outputFolder, studyPopFile),
                    riskPopFile = file.path(outputFolder, refRow$riskPopFile),
                    outcomeModelFile = file.path(outputFolder, refRow$outcomeModelFile)))
}

# Copied from inside CohortMethod::RunAnalysis.R
createArgs <- function(i) {
        refRow <- subset[i, ]
        analysisRow <- ParallelLogger::matchInList(cmAnalysisList,
                                                   list(analysisId = refRow$analysisId))[[1]]
        analysisRow$fitOutcomeModelArgs$control$threads <- 1
        analysisRow$createStudyPopArgs$outcomeId <- refRow$outcomeId
        prefilteredCovariatesFile <- refRow$prefilteredCovariatesFile
        if (prefilteredCovariatesFile != "") {
                prefilteredCovariatesFile = file.path(outputFolder, refRow$prefilteredCovariatesFile)
        }
        
        riskPopFile <- refRow$riskPopFile
        if (riskPopFile != "") {
                riskPopFile <- file.path(outputFolder, riskPopFile)
        }
        
        params <- list(cohortMethodDataFile = file.path(outputFolder, refRow$cohortMethodDataFile),
                       prefilteredCovariatesFile = prefilteredCovariatesFile,
                       sharedPsFile = file.path(outputFolder, refRow$sharedPsFile),
                       riskPopFile = riskPopFile,
                       args = analysisRow,
                       outcomeModelFile = file.path(outputFolder, refRow$outcomeModelFile))
        return(params)
}


# Execute for outcomes of interest

subset <- omf[omf$outcomeOfInterest,]

for (idx in c(1:nrow(subset))) {

        # for one entry
        cat(idx)

        modelToFit <- createOutcomeModelTask(idx) # Uses subset
        modelToFit$args$modelType <- "fgr"

        # studyPop <- readRDS(modelToFit$studyPopFile)
        # riskPop <- readRDS(modelToFit$riskPopFile)
        # unknowns <- setdiff(riskPop$subjectId, studyPop$subjectId)
        # riskPop <- riskPop[!(riskPop$subjectId %in% unknowns), ]
        # combPop <- combineCompetingStudyPopulations(studyPop, riskPop)

        cohortMethodData <- Andromeda::loadAndromeda(modelToFit$cohortMethodDataFile)
        CohortMethod:::doFitOutcomeModel(modelToFit)
        Andromeda::close(cohortMethodData)
}

# Execute for negative controls

subset <- omf[!omf$outcomeOfInterest, ]

for (idx in c(1:nrow(subset))) {
        # idx <- 1
        
        # for one entry
        cat(idx)
        
        modelToFit <- createArgs(idx) # Uses subset
        modelToFit$args$modelType <- "fgr"
        
        cohortMethodData <- Andromeda::loadAndromeda(modelToFit$cohortMethodDataFile)
        CohortMethod:::doFitOutcomeModelPlus(modelToFit)
        Andromeda::close(cohortMethodData)
}

# Update master file

newOmf <- omf %>% mutate(analysisId = analysisId + 7)
totalOmd <- rbind(readRDS(file.path(savedOutputFolder, "cmOutput/outcomeModelReference.rds")) %>%
                          mutate(riskId = -1,
                                 riskPopFile = ""),
                  newOmf)
saveRDS(totalOmd, file.path(savedOutputFolder, "cmOutput/outcomeModelReference.rds"))

outputFolder <- savedOutputFolder

# Just package up the results

execute(connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        oracleTempSchema = oracleTempSchema,
        outputFolder = outputFolder,
        databaseId = databaseId,
        databaseName = databaseName,
        databaseDescription = databaseDescription,
        createCohorts = FALSE,
        synthesizePositiveControls = FALSE,
        runAnalyses = FALSE,
        packageResults = TRUE,
        maxCores = maxCores)
