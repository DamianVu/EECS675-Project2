
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
	double purchaseAmount;
};

void printModel(const ModelData& data) {
	std::cout << "{modelNumber: " << data.model << ", date: " << data.purchaseDate << ", customerType: " << data.customer << ", amount: " << data.purchaseAmount << "}\n";
}

std::string errorCode(int tag);

void defineStructDataToMPI(MPI_Datatype * typeToBeCreated);

void rank0(int communicatorSize, std::string filename, int reportYear, char customerType, MPI_Comm dataComm) {
	int numRecords, numModels;
	std::fstream input(filename);
	input >> numRecords >> numModels;

	ModelData records[numRecords];

	for (int i = 0; i < numRecords; i++) {
		int modelNum, date;
		char customer;
		double purchaseAmountInDollars;
		input >> modelNum >> date >> customer >> purchaseAmountInDollars;
		records[i].model = modelNum;
		records[i].purchaseDate = date;
		records[i].customer = customer;
		records[i].purchaseAmount = purchaseAmountInDollars;
	}

	MPI_Datatype ModelDataStruct;
	defineStructDataToMPI(&ModelDataStruct);

	MPI_Request sendReq[2];

	// Send data to all other processes;

	for (int i = 1; i < communicatorSize - 1; i++) {
		int numRecordsToProcess = numRecords / (communicatorSize - 2);
		int num = numRecordsToProcess;
		if (i == communicatorSize - 2) {
			numRecordsToProcess += numRecords % (communicatorSize - 2);
		}
		MPI_Isend(&numRecordsToProcess, 1, MPI_INT, i, 0, dataComm, &sendReq[0]);
		MPI_Isend(&records[(i - 1) * num], numRecordsToProcess, ModelDataStruct, i, 1, dataComm, &sendReq[1]);
	}

	ModelData f;
	MPI_Isend(&f, 1, ModelDataStruct, 1, 0, MPI_COMM_WORLD, &sendReq[0]);



}

void ranki(int communicatorSize, int rank, MPI_Comm dataComm) {
	int recordsToProcess;
	MPI_Recv(&recordsToProcess, 1, MPI_INT, 0, 0, dataComm, MPI_STATUS_IGNORE);
	std::cout << "Rank " << rank-1 << " should process " << recordsToProcess << " records\n";

	MPI_Datatype ModelDataStruct;
	defineStructDataToMPI(&ModelDataStruct);

	ModelData * data = new ModelData[recordsToProcess];
	MPI_Recv(data, recordsToProcess, ModelDataStruct, 0, 1, dataComm, MPI_STATUS_IGNORE);
	std::cout << "Rank " << rank-1 << " received its chunk of model data\n";

	// Process our chunk of data

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

	std::cout << "Error reported finished\n";
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
		ranki(N, rank, dataComm);
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
	types[3] = MPI_DOUBLE;

	MPI_Type_create_struct(NUM_DATA_FIELDS, blklen, displ, types, typeToBeCreated);
	MPI_Type_commit(typeToBeCreated);

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
	}
}
