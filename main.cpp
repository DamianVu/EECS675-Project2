
#include <mpi.h>
#include <iostream>
#include <fstream>
#include <string>
#include <cstdlib>

static const int NUM_DATA_FIELDS = 4;

struct ModelData {
	int model;
	int purchaseDate;
	char customer;
	float purchaseAmount;
};

void printModel(const ModelData & data) {
	std::cout << "{modelNumber: " << data.model << ", date: " << data.purchaseDate << ", customerType: " << data.customer << ", amount: " << data.purchaseAmount << "}\n";
}

int checkForErrors(const ModelData & data, int numModels); 

std::string errorCode(int tag);

void defineStructDataToMPI(MPI_Datatype * typeToBeCreated);

void rank0(int communicatorSize, std::string filename, int reportYear, char customerType, MPI_Comm dataComm) {
	MPI_Datatype ModelDataStruct;
	defineStructDataToMPI(&ModelDataStruct);
	MPI_Request sendReq[2];
	ModelData f;

	int numRecords, numModels;
	std::ifstream input(filename);

	// Check if we have any command line argument errors
	bool badInput = !input;
	bool badYear = reportYear < 1997 || reportYear > 2018;
	bool badCustomerType = !(customerType == 'G' || customerType == 'I' || customerType == 'R');
	if (badInput || badYear || badCustomerType) {
		if (badInput)
			std::cout << "Error: Couldn't open file " << filename << "\n";
		if (badYear)
			std::cout << "Error: " << reportYear << " is not a valid year\n";
		if (badCustomerType)
			std::cout << "Error: " << customerType << " is not a valid customer type\n";

		// Tell rank l processes to end
		int initData[3];
		initData[0] = -1;
		for (int i = 1; i < communicatorSize - 1; i++)
			MPI_Isend(&initData, 3, MPI_INT, i, 0, dataComm, &sendReq[0]);

		// Tell rank 1 process (error reporter) to end
		MPI_Isend(&f, 1, ModelDataStruct, 1, 0, MPI_COMM_WORLD, &sendReq[1]);

		MPI_Finalize();
		exit(0);
	}
	input >> numRecords >> numModels;

	int num = numRecords / (communicatorSize - 2);
	int rem = numRecords % (communicatorSize - 2);

	int totalRecords = (num + rem) * (communicatorSize - 2);

	ModelData * records = new ModelData[totalRecords];

	for (int i = 0; i < totalRecords; i++) {
		if ((i % (num + rem)) - num >= 0) {
			records[i].model = -1;
		} else {
			int modelNum, date;
			char customer;
			float purchaseAmountInDollars;
			input >> modelNum >> date >> customer >> purchaseAmountInDollars;
			records[i].model = modelNum;
			records[i].purchaseDate = date;
			records[i].customer = customer;
			records[i].purchaseAmount = purchaseAmountInDollars;
		}
	}

	// Send data to all other processes;
	int recordsToProcess = num + rem;

	for (int i = 1; i < communicatorSize - 1; i++) {
		int numRecordsToProcess = num;
		if (i == communicatorSize - 2) {
			numRecordsToProcess += rem;
		}

		int initData[3];
		initData[0] = numRecordsToProcess;
		initData[1] = numModels;
		initData[2] = recordsToProcess;
		MPI_Isend(&initData, 3, MPI_INT, i, 0, dataComm, &sendReq[0]);
	}

	MPI_Scatter(&records, recordsToProcess, ModelDataStruct, MPI_IN_PLACE, recordsToProcess, ModelDataStruct, 0, dataComm);

	float * entireFile = new float[numModels];
	float * givenYear = new float[numModels];
	float * forCustomer = new float[numModels];
	float * dummy = new float[numModels];

	MPI_Reduce(dummy, entireFile, numModels, MPI_FLOAT, MPI_SUM, 0, dataComm);
	MPI_Reduce(dummy, givenYear, numModels, MPI_FLOAT, MPI_SUM, 0, dataComm);
	MPI_Reduce(dummy, forCustomer, numModels, MPI_FLOAT, MPI_SUM, 0, dataComm);

	MPI_Ssend(&f, 1, ModelDataStruct, 1, 0, MPI_COMM_WORLD);

	// Make sure that the error reporting is finished.
	//int temp;
	//MPI_Recv(&temp, 1, MPI_INT, 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);


	// Final report
	std::cout << "\n\nFinal Report:\n\n===============\n";
	std::cout << "Model #\t\tTotal Amounts\tAmounts for " << reportYear << "\tAmounts for customer type " << customerType << "\n";
	for (int i = 0; i < numModels; i++) {
		std::cout << i << "\t\t" << entireFile[i] << "\t\t" << givenYear[i] << "\t\t\t" << forCustomer[i] << "\n";
	}

}

void ranki(int reportYear, char customerType, MPI_Comm dataComm) {
	int initData[2];
	MPI_Recv(&initData, 3, MPI_INT, 0, 0, dataComm, MPI_STATUS_IGNORE);
	int recordsToProcess = initData[0];
	int numModels = initData[1];
	int fullRecord = initData[2];
	if (recordsToProcess == -1) {
		// Error.
		MPI_Finalize();
		exit(0);
	}

	MPI_Datatype ModelDataStruct;
	defineStructDataToMPI(&ModelDataStruct);

	ModelData * data = new ModelData[fullRecord];
	MPI_Scatter(&data, fullRecord, ModelDataStruct, &data, fullRecord, ModelDataStruct, 0, dataComm);

	MPI_Request req;

	// Process our chunk of data
	float * entireFile = new float[numModels];
	float * givenYear = new float[numModels];
	float * forCustomer = new float[numModels];

	for (int i = 0; i < numModels; i++) {
		entireFile[i] = 0.0;
		givenYear[i] = 0.0;
		forCustomer[i] = 0.0;
	}

	for (int i = 0; i < recordsToProcess; i++) {
		int error = checkForErrors(data[i], numModels);
		if (error == 0) {
			// Process it
			entireFile[data[i].model] += data[i].purchaseAmount;
			std::string date = std::to_string(data[i].purchaseDate);
			int year = std::stoi(date.substr(0,4));
			if (year == reportYear)
				givenYear[data[i].model] += data[i].purchaseAmount;
			if (data[i].customer == customerType)
				forCustomer[data[i].model] += data[i].purchaseAmount;
		} else {
			// Report error
			MPI_Isend(&data[i], 1, ModelDataStruct, 1, error, MPI_COMM_WORLD, &req);
		}
	}

	float * final = new float[numModels];

	MPI_Reduce(entireFile, final, numModels, MPI_FLOAT, MPI_SUM, 0, dataComm);
	MPI_Reduce(givenYear, final, numModels, MPI_FLOAT, MPI_SUM, 0, dataComm);
	MPI_Reduce(forCustomer, final, numModels, MPI_FLOAT, MPI_SUM, 0, dataComm);

}

void errorReporter() {
	MPI_Datatype ModelDataStruct;
	defineStructDataToMPI(&ModelDataStruct);

	ModelData data;
	MPI_Status status;
	
	while (true) {
		MPI_Recv(&data, 1, ModelDataStruct, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		if (status.MPI_SOURCE == 0) {
			break;
		} else {
			std::cout << "Rank " << status.MPI_SOURCE << " reported " << errorCode(status.MPI_TAG) << ": ";
			printModel(data);
		}
	}
	//int temp = 1;
	//MPI_Ssend(&temp, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
}

int main(int argc, char** argv) {
	MPI_Init(&argc, &argv);

	int rank, N;

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &N);

	int dataRank, dataSize;
	MPI_Comm dataComm;
	int color = 0;
	if (rank != 1)
		color = 1;
	MPI_Comm_split(MPI_COMM_WORLD, color, rank, &dataComm);
	MPI_Comm_rank(dataComm, &dataRank);
	MPI_Comm_size(dataComm, &dataSize);

	if (rank == 0) {
		rank0(N, argv[1], atoi(argv[2]), argv[3][0], dataComm);
	} else if (rank == 1) {
		errorReporter();
	} else {
		ranki(atoi(argv[2]), argv[3][0], dataComm);
	}


	MPI_Finalize();
}

void defineStructDataToMPI(MPI_Datatype * typeToBeCreated) {
	int blklen[NUM_DATA_FIELDS];
	for (int i = 0; i < NUM_DATA_FIELDS; i++) {
		blklen[i] = 1;
	}

	MPI_Aint displ[NUM_DATA_FIELDS];
	MPI_Datatype types[NUM_DATA_FIELDS];

	displ[0] = 0;

	ModelData sample;

	MPI_Aint base;
	MPI_Get_address(&sample.model, &base);

	MPI_Aint oneField;

	MPI_Get_address(&sample.purchaseDate, &oneField);
	displ[1] = oneField - base;

	MPI_Get_address(&sample.customer, &oneField);
	displ[2] = oneField - base;

	MPI_Get_address(&sample.purchaseAmount, &oneField);
	displ[3] = oneField - base;

	types[0] = MPI_INT;
	types[1] = MPI_INT;
	types[2] = MPI_CHAR;
	types[3] = MPI_FLOAT;

	MPI_Type_create_struct(NUM_DATA_FIELDS, blklen, displ, types, typeToBeCreated);
	MPI_Type_commit(typeToBeCreated);

}

int checkForErrors(const ModelData & data, int numModels) {
	if (data.model < 0 || data.model >= numModels)
		return 1;
	std::string date = std::to_string(data.purchaseDate);
	if (date.length() != 6)
		return 2;
	int year = std::stoi(date.substr(0,4));
	int month = std::stoi(date.substr(4,2));
	if (month < 1 || month > 12)
		return 3;
	if (year < 1997 || year > 2018)
		return 4;
	char c = data.customer;
	if (!(c == 'G' || c == 'I' || c == 'R'))
		return 5;
	if (data.purchaseAmount < 0.0)
		return 6;
	return 0;
}

std::string errorCode(int tag) {
	switch (tag) {
		case 1:
		return "\"Bad model number\"";
		case 2:
		return "\"Bad date\"";
		case 3:
		return "\"Invalid month\"";
		case 4:
		return "\"Date outside of possible range\"";
		case 5:
		return "\"Invalid customer type\"";
		case 6:
		return "\"Amount of sale < 0\"";
		default:
		return "\"?????\"";
	}
}
