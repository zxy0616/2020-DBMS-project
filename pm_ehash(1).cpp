#include"pm_ehash.h"
#include <utility>
#include <cmath>
#include <fstream>
#include <string>
#include <iostream>
#include <sstream>
#include <queue>
#include <libpmem.h>
#include <stdint.h>
#include <sstream>

typedef struct iofCatalog{
	int size;
	uint32_t Fileid[100];
	uint32_t Offset[100];
}iofCatalog;

typedef struct iofMetadata {
	uint64_t sum_of_bucket;		//Ͱ����Ŀ
	uint64_t max_file_id;      // next file id that can be allocated
	uint64_t catalog_size;     // the catalog size of catalog file(amount of data entry)
	uint64_t global_depth;
}iofMetadata;


typedef struct iofDatapage {
	uint32_t fileid;
	uint8_t  bitmap[32];
	pm_bucket slot[32];
}iofDatapage;

/**
 * @description: construct default for PmHash 
 * @param NULL
 * @return: a initial instance of PmEHash
 */

//#define DATA_PAGE_SLOT_NUM 32
//#define BUCKET_SLOT_NUM 15
//#define DEFAULT_CATALOG_SIZE 16
PmEHash::PmEHash() {
	ifstream filein;
	filein.open("catalog");
	if(filein.is_open()){ //catalog exits in file, initialize PmHash according to the file
		filein.close();
		recover();
	}

	else{
		data_page oriDataPage;
		oriDataPage.fileid = 1;

		metadata = new ehash_metadata;
		metadata->sum_of_bucket = 16;
		metadata->catalog_size = 16;
		metadata->global_depth = 4;
		metadata->max_file_id = 2;

		for (int i = 0; i < DATA_PAGE_SLOT_NUM; i++)
			oriDataPage.bitmap[i] = 0;//initialize Datapage bitmap with 0

			for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i) {
			pm_bucket temp;
			temp.local_depth = 4;//Each bucket store less than 2^4
			temp.bitmap[0] = temp.bitmap[1] = 0;//initialize two size8 bitmap with 0
			oriDataPage.slot.push_back(temp);//push default 32 buckets 
		}
		datapage.push_back(oriDataPage); //datepage is a series of single DataPage

		catalog.buckets_virtual_address = new pm_bucket * [DEFAULT_CATALOG_SIZE];
		for (int i = 0; i < DEFAULT_CATALOG_SIZE; ++i) 
			catalog.buckets_virtual_address[i] = &datapage[0].slot[i]; //set virtual address

	
		catalog.buckets_pm_address = new pm_address[DEFAULT_CATALOG_SIZE];
		for (int i = 0; i < DEFAULT_CATALOG_SIZE; ++i) {
			catalog.buckets_pm_address[i].fileId = oriDataPage.fileid;
			catalog.buckets_pm_address[i].offset = i * sizeof(pm_bucket);
		}

		for (int i = DEFAULT_CATALOG_SIZE; i < DATA_PAGE_SLOT_NUM; ++i)
			free_list.push(&datapage[0].slot[i]);

		for (int i = 0; i < DEFAULT_CATALOG_SIZE; ++i) {
			vAddr2pmAddr.insert(pair<pm_bucket*, pm_address>(&datapage[0].slot[i], catalog.buckets_pm_address[i]));
			pmAddr2vAddr.insert(pair<pm_address, pm_bucket*>(catalog.buckets_pm_address[i], &datapage[0].slot[i]));
		}
	}
}
/**
 * @description: Deconstruct function for PmHash
 * @param NULL
 * @return: NULL
 */
PmEHash::~PmEHash() {
	//catalog iof
	size_t csize;
	int pmc;
	iofCatalog* Clogtemp = (iofCatalog*)pmem_map_file("catalog",sizeof(iofCatalog),PMEM_FILE_CREATE,0777,&csize,&pmc);
	Clogtemp->size = metadata->catalog_size;
	for(int i = 0; i < metadata->catalog_size; ++i) {
		Clogtemp->Fileid[i] = catalog.buckets_pm_address[i].fileId;
		Clogtemp->Offset[i] = catalog.buckets_pm_address[i].offset;
	}
	
	pmem_persist(Clogtemp, csize);
	pmem_unmap(Clogtemp, csize);

	//metadata iof
	size_t msize;
	int pmm;
	iofMetadata * Metatemp =(iofMetadata*) pmem_map_file("metadata", sizeof(iofMetadata), PMEM_FILE_CREATE, 0777, &msize, &pmm);
	Metatemp->sum_of_bucket = metadata->sum_of_bucket;
	Metatemp->global_depth = metadata->global_depth;
	Metatemp->max_file_id = metadata->max_file_id;
	Metatemp->catalog_size = metadata->catalog_size;
	pmem_persist(Metatemp, msize);
	pmem_unmap(Metatemp, msize);
	
	//datapage iof
	for (int i = 1; i <= datapage.size(); ++i) {
		string tempstr;
		stringstream sstream;
		sstream << i;
		sstream >> tempstr;
		const char* name = tempstr.c_str();
		size_t dsize;
		int pmd;
		iofDatapage * pagetemp =(iofDatapage*) pmem_map_file(name, sizeof(iofDatapage), PMEM_FILE_CREATE, 0777, &dsize, &pmd);
		pagetemp->fileid = datapage[i - 1].fileid;
		for (int j = 0; j < DATA_PAGE_SLOT_NUM; ++j) {
			pagetemp->bitmap[j] = datapage[i - 1].bitmap[j];
		}
		//pm_bucket temp_b;
		for (int j = 0; j < datapage[i - 1].slot.size(); ++j) {
			pagetemp->slot[j].bitmap[0] = datapage[i - 1].slot[j].bitmap[0];
			pagetemp->slot[j].bitmap[1] = datapage[i - 1].slot[j].bitmap[1];
			pagetemp->slot[j].local_depth = datapage[i - 1].slot[j].local_depth;
			for (int k = 0; k < 15; ++k) {
				pagetemp->slot[j].slot[k].key = datapage[i - 1].slot[j].slot[k].key;
				pagetemp->slot[j].slot[k].value = datapage[i - 1].slot[j].slot[k].value;
			}
			//tdatapage->slot.push_back(temp_b);
		}
		pmem_persist(pagetemp, dsize);
		pmem_unmap(pagetemp, dsize);
	}
}

/**
 * @description: insert a new kv , and update the correspondedding bitmap to 1
 * @param kv: an new kv
 * @return: return 0 for success
 			return -1 for already existing
 */
int PmEHash::insert(kv item) {
	uint64_t value;
	if (search(item.key, value) == 0) return -1; //exist already
	//no exist, begin for insertion
	pm_bucket* targetBucket = getFreeBucket(item.key);
	kv* targetSlot = getFreeKvSlot(targetBucket);
	*targetSlot = item;

	int theoffset = (targetSlot - targetBucket->slot);
	if ( !(theoffset > 8) ) targetBucket->bitmap[0] += pow(2, 8 - 1 - theoffset); //down 8 bits
	else
		targetBucket->bitmap[1] += pow(2, 16 - 1 - theoffset); //UP 8 bits
	return 0; //success insertion
}

/**
 * @description: delete a kv and update the correspondding bitmap to 0
 * @param uint64_t: the kv key to be deleted
 * @return: return 0 for success removement
 			return -1 for fail as no existing before
 */
int PmEHash::remove(uint64_t item_key) {
	uint64_t order_of_bucket = hashFunc(item_key);
	pm_bucket *vaddress_of_bucket = catalog.buckets_virtual_address[order_of_bucket]; 
	
	
	int total;
	int number;
	if(vaddress_of_bucket->bitmap[1] > 0 ){
		total = 16;
		number = vaddress_of_bucket->bitmap[1];
	}
	else{
		total = 8;
		number = vaddress_of_bucket->bitmap[0];
	}
	//number = vaddress_of_bucket->bitmap[1] > 0 ? vaddress_of_bucket->bitmap[1] : vaddress_of_bucket->bitmap[0];
	if (number == 0 && total == 8) total = 0; //all 0, bitmap -> low  00000000 00000000  high
	else if (number == 0 && total == 16) total = 8; // not all 0, bitmap -> low ******** 00000000 high
	else while (number % 2 != 1) {
			total--;
			number = number/2;
		 }// high/low each not all zero
	int order_of_slot = -1;
	for (int i = 0; i < total; i++) {
		
		uint8_t temp;
		int highest;
		if( i < 8) {
			highest = 7;
			temp = vaddress_of_bucket->bitmap[0];
		}
		else{
			highest = 15;
			temp = vaddress_of_bucket->bitmap[1];
		}
		
		if ((temp >> (highest-i))%2 == 1 && vaddress_of_bucket->slot[i].key == item_key) {
			order_of_slot = i;
			break;
		}
	}	
	
	if (order_of_slot == -1) return -1; //no existing
	
	// bitmap to 0
	if (order_of_slot <= 7) vaddress_of_bucket->bitmap[0] -= pow(2, 8- 1 - order_of_slot); //set corresponding bitmap to 0
	else
		vaddress_of_bucket->bitmap[0] -= pow(2, 15 - order_of_slot);//set corresponding bitmap to 0
	
	if(vaddress_of_bucket->bitmap[1] == 0 && vaddress_of_bucket->bitmap[1] == 0)
		mergeBucket(order_of_bucket); //if bucket empty, then merge it
	return 0;
}
/**
 * @description: update the value for an already existing kv's key
 * @param kv: a new kv, same key but different value
 * @return: return 0 for success removement
 			return -1 for fail as no existing before
 */
int PmEHash::update(kv item) {
	uint64_t order_of_bucket = hashFunc(item.key);
	pm_bucket *vaddress_of_bucket = catalog.buckets_virtual_address[order_of_bucket]; 
	
	//num of data in slot
	int total;
	int num; 
	if(vaddress_of_bucket->bitmap[1] > 0){
		total = 16;
		num = vaddress_of_bucket->bitmap[1];
	}
	else{
		total = 8;
		num = vaddress_of_bucket->bitmap[0];
	}
	if(num == 0 && total == 8 ) total = 0;
	else if(num == 0 && total == 16)total = 8;
	else while( num%2 != 1){
		--total;
		num = num/2;
	}
	//fix the position and updata the value    
	for(int i = 0 ; i < total ; ++i){
		uint8_t temp;
		int highest;
		if( i<= 7){
			temp = vaddress_of_bucket->bitmap[0];
			highest = 7;
		}
		else{
			temp = vaddress_of_bucket->bitmap[1];
			highest = 15;
		}

		if( (temp >> (highest-i)) == 1 && vaddress_of_bucket->slot[i].key == item.key ){
			vaddress_of_bucket->slot[i].value = item.value;
			return 0;
		}
	}
	return -1;
}
/**
 * @description: input the given key and search it in buckets and return the correspondding value
 * @param uint64_t: the key the of target kv
 * @param uint64_t&: target kv's value
 * @return: return 0 for success removement
 			return -1 for fail as no existing
 */
int PmEHash::search(uint64_t key, uint64_t& return_val) {
	uint64_t order_of_bucket = hashFunc(key);
	pm_bucket *vaddress_of_bucket = catalog.buckets_virtual_address[order_of_bucket]; 
	
	
	int total;
	int num;
	//num of kv in the slot
	if(vaddress_of_bucket->bitmap[1] > 0){
		total = 16;
		num = vaddress_of_bucket->bitmap[1];
	}
	else{
		total = 8;
		num = vaddress_of_bucket->bitmap[0];
	}
	if(num == 0 && total == 8 ) total = 0;
	else if(num == 0 && total == 16)total = 8;
	else while( num%2 != 1){
		--total;
		num = num/2;
	}
	
	//ergodic and make it out
	for (int i = 0; i < total; i++) {
		uint8_t temp;
		int highest;
		if( i<= 7){
			temp = vaddress_of_bucket->bitmap[0];
			highest = 7;
		}
		else{
			temp = vaddress_of_bucket->bitmap[1];
			highest = 15;
		}

		if( (temp >> (highest-i)) == 1 && vaddress_of_bucket->slot[i].key == key ){
			return_val = vaddress_of_bucket->slot[i].value;
			return 0;
		}
	}
	return -1;
}

/**
 * @description: with Hash Algorithm, get the correspondding order number of a bucket 
 * @param uint64_t: a kv's key
 * @return: the order among the buckets
 */
uint64_t PmEHash::hashFunc(uint64_t key) {
	int temp;
	int bucketorder;
	for (int i = 4; i <= metadata->global_depth; ++i) {
		temp = (1 << i);
		bucketorder = key % temp;
		if (catalog.buckets_virtual_address[bucketorder]->local_depth == i)return bucketorder;
	}
}

/**
 * @description: with the key, find the target order of bucket, eixst then return its virtual address, or split and return a new bucket's virtual address
 * @param uint64_t: a kv's key
 * @return: virtual address of the target bucket
 */
pm_bucket* PmEHash::getFreeBucket(uint64_t key) {
	uint64_t order_of_bucket = hashFunc(key);
	pm_bucket* vaddr_bucket = catalog.buckets_virtual_address[order_of_bucket];
	while ((*vaddr_bucket).bitmap[0] == 255 && (*vaddr_bucket).bitmap[1] == 254) {
		splitBucket(order_of_bucket);
		uint64_t temp = 1;
		temp = (temp << (vaddr_bucket->local_depth-1));
		order_of_bucket += temp;

		vaddr_bucket = catalog.buckets_virtual_address[order_of_bucket];
	}

	return catalog.buckets_virtual_address[order_of_bucket];

}

pm_bucket* PmEHash::getNewBucket()
{
	return nullptr;
}

/**
 * @description: get the first avaliable pos for the kv to insert 
 * @param pm_bucket* bucket: the bucket's virtual address
 * @return: the first avaliable pos's virtual addrss
 */
kv* PmEHash::getFreeKvSlot(pm_bucket* bucket) {
	int total;
	int num;
	//num of kv in the slot
	if(bucket->bitmap[1] > 0){
		total = 16;
		num = bucket->bitmap[1];
	}
	else{
		total = 8;
		num = bucket->bitmap[0];
	}
	if (num == 0 && total == 8) return &bucket->slot[0];
	if (num == 0 && total == 16) return &bucket->slot[8];
	while (num % 2 != 1) {
		total--;
		num = num/2;
	}
	for (int i = 0; i < total; i ++) {
		uint8_t temp;
		int highest;
		if( i<= 7){
			temp = bucket->bitmap[0];
			highest = 7;
		}
		else{
			temp = bucket->bitmap[1];
			highest = 15;
		}
		
		if ((temp >> (highest-i))%2 == 0) {
			return &bucket->slot[i]; 
		}
	}
	return &bucket->slot[total];
}

/**
 * @description: split the bucket, if datapage is full, then create a new datapage also
 * @param uint64_t: oreder of bucket in a datapage
 * @return: NULL
 */
void PmEHash::splitBucket(uint64_t bucket_id) {
	//uint64_t temp = *catalog.buckets_virtual_address[bucket_id].local_depth;
	metadata->sum_of_bucket++;
	int offset = metadata->sum_of_bucket % DATA_PAGE_SLOT_NUM;
	uint64_t temp = catalog.buckets_virtual_address[bucket_id]->local_depth;
	if (temp == metadata->global_depth) {
		extendCatalog();
	}
	catalog.buckets_virtual_address[bucket_id]->local_depth++;
	uint64_t num = (1 << temp);
	uint64_t page_num = bucket_id + num;
	pm_address new_address = catalog.buckets_pm_address[page_num];
	catalog.buckets_virtual_address[page_num] = (pm_bucket*)getFreeSlot(new_address);
	catalog.buckets_pm_address[page_num].fileId = metadata->max_file_id - 1;
	catalog.buckets_pm_address[page_num].offset = (offset - 1) * sizeof(pm_bucket);
	//catalog.buckets_virtual_address[page_num]->local_depth = temp + 1;
	vAddr2pmAddr.insert(pair<pm_bucket*, pm_address>(catalog.buckets_virtual_address[page_num], new_address));
	pmAddr2vAddr.insert(pair<pm_address, pm_bucket*>(new_address, catalog.buckets_virtual_address[page_num]));
	//catalog.buckets_virtual_address[page_num]->local_depth = temp + 1;
	catalog.buckets_virtual_address[bucket_id]->bitmap[0] = 0;
	catalog.buckets_virtual_address[bucket_id]->bitmap[1] = 0;

	catalog.buckets_virtual_address[page_num]->local_depth =temp +1;
	for (int i = 0; i < 15; ++i) {
		kv temp = catalog.buckets_virtual_address[bucket_id]->slot[i];
		insert(temp);
	}
}

/**
 * @description: if a bucket(not basic) is empty, then merge it
 * @param uint64_t: order of the bucket
 * @return: NULL
 */
void PmEHash::mergeBucket(uint64_t bucket_id) {
	if (bucket_id >= 0 && bucket_id <= 15) return; //basic buckets
	else {
		int order_of_slot;
		pm_bucket *vaddr_bucket = catalog.buckets_virtual_address[bucket_id]; 
		
		bool status = false; //A bucket have splited or not
		uint64_t depth = vaddr_bucket->local_depth;
		while(1) {
			if (bucket_id >= pow(2,depth-1) && bucket_id < pow(2,depth)) break;
			depth--;
			status = true;
		}
		//have Splited
		if (status) {
			uint64_t next_bucket_id = bucket_id + pow(2,vaddr_bucket->local_depth-1);
			pm_bucket *next_vaddr_bucket = catalog.buckets_virtual_address[next_bucket_id];
			vaddr_bucket->local_depth--;
			// num of kv in slot
			int total;
			int num;
			//num of kv in the slot
			if(vaddr_bucket->bitmap[1] > 0){
				total = 16;
				num = vaddr_bucket->bitmap[1];
			}
			else{
				total = 8;
				num = vaddr_bucket->bitmap[0];
			}
			if (num == 0 && total == 8) total = 0;
			if (num == 0 && total == 16) total = 8;
			while (num % 2 != 1) {
				total--;
				num = num/2;
			}
			//kv in splited bucket again into the basic bucket
			for (int i = 0; i < total; i++) {
				uint8_t temp;
				int highest;
				if( i<= 7){
					temp = vaddr_bucket->bitmap[0];
					highest = 7;
				}
				else{
					temp = vaddr_bucket->bitmap[1];
					highest = 15;
				}
				if ((temp >> (highest-i))%2 == 1) {
					insert(next_vaddr_bucket->slot[i]);
				}
			}
			//set the next bucket's bitmap to 0
			next_vaddr_bucket->bitmap[0] = next_vaddr_bucket->bitmap[1] = 0;
			catalog.buckets_virtual_address[next_bucket_id] = NULL;
			order_of_slot = catalog.buckets_pm_address[next_bucket_id].offset / sizeof(pm_bucket); 
		}
	
		//never splited
		else{
			uint64_t last_bucket_id = bucket_id - pow(2,vaddr_bucket->local_depth-1);
			pm_bucket *last_bucket = catalog.buckets_virtual_address[last_bucket_id];
			//same local_depth, merge
			if (vaddr_bucket->local_depth == last_bucket->local_depth) {
				last_bucket->local_depth--;
				catalog.buckets_virtual_address[bucket_id] = NULL;
				order_of_slot = catalog.buckets_pm_address[bucket_id].offset / sizeof(pm_bucket);
			}
			else
				order_of_slot = -1;//diff local_depth, keep
		}
		//pmAddr2vAddr.erase(it);
		
		//on Datapage, delete the bucket
		if (order_of_slot != -1) {
			queue<pm_bucket*> list;
			list.push(&datapage[catalog.buckets_pm_address[bucket_id].fileId - 1].slot[order_of_slot]);
			pm_bucket *temp;
			for (int i = 0; i < free_list.size(); i++) {
				temp = free_list.front();
				free_list.pop();
				list.push(temp);
			}
			free_list = list;
			//set corresponding bitmap to zero
			datapage[catalog.buckets_pm_address[bucket_id].fileId - 1].bitmap[order_of_slot] = 0;
			metadata->sum_of_bucket--;
		}
		
	}
}

/**
 * @description: if slot is full, double size the catalog(datapage)
 * @param NULL
 * @return: NULL
 */
void PmEHash::extendCatalog() {
	int size = metadata->catalog_size * 2;
	ehash_catalog new_catalog;
	new_catalog.buckets_pm_address = new pm_address[size];
	new_catalog.buckets_virtual_address = new pm_bucket * [size];
	
	for (int i = 0; i < size / 2; i++) {
		new_catalog.buckets_pm_address[i] = catalog.buckets_pm_address[i];
		new_catalog.buckets_virtual_address[i] = catalog.buckets_virtual_address[i];
	}
	
	delete[]catalog.buckets_pm_address;
	delete[]catalog.buckets_virtual_address;
	catalog = new_catalog;
	
	metadata->global_depth++;
	metadata->catalog_size *= 2;
}

/**
 * @description: get an avaliable slot and give back its address
 * @param pm_address&: to bring out the target address
 * @return: an address of the target slot
 */
void* PmEHash::getFreeSlot(pm_address& new_address) {
	if (free_list.size() == 0)
		allocNewPage();

	pm_bucket* temp = free_list.front();
	free_list.pop();
	return temp;
}

/**
 * @description: allocate an new address for the new Data Page
 * @param NULL
 * @return: NULL
 */
void PmEHash::allocNewPage() {
	data_page newone;
	newone.fileid = datapage.size() + 1;
	datapage.push_back(newone);
	for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i){
		datapage[datapage.size() - 1].bitmap[i] = 0;//set new page's bitmap to 0
		pm_bucket temp;
		temp.bitmap[0] = 0;
		temp.bitmap[1] = 0;
		datapage[datapage.size() - 1].slot.push_back(temp);
		free_list.push(&(datapage[datapage.size() - 1].slot[i]));
	}
	metadata->max_file_id++;
}

/**
 * @description: 
 * @param NULL
 * @return: NULL
 */
void PmEHash::recover() {
	//catelog iof
	size_t csize;
	int pmc;
	iofCatalog* catetemp = (iofCatalog*)pmem_map_file("catalog", sizeof(iofCatalog), PMEM_FILE_CREATE, 0777, &csize, &pmc);
	catalog.buckets_pm_address = new pm_address[catetemp->size];
	catalog.buckets_virtual_address = new pm_bucket * [catetemp->size];
	for (int i = 0; i < catetemp->size; ++i) {
		catalog.buckets_pm_address[i].fileId = catetemp->Fileid[i];
		catalog.buckets_pm_address[i].offset = catetemp->Offset[i];
	}
	//cout << catasize;//->size;
	pmem_unmap(catetemp, csize);
	
	//metadata iof
	size_t msize;
	int pmm;
	iofMetadata* metatemp = (iofMetadata*)pmem_map_file("metadata", sizeof(iofMetadata), PMEM_FILE_CREATE, 0777, &msize, &pmm);
	metadata->catalog_size = metatemp->catalog_size;
	metadata->global_depth = metatemp->global_depth;
	metadata->max_file_id = metatemp->max_file_id;
	metadata->sum_of_bucket = metatemp->sum_of_bucket;
	pmem_unmap(metatemp, msize);
	
	//datapage iof
	for (int i = 1; i < metadata->max_file_id ; ++i) {
		size_t psize;
		int pmd;
		string tempstr;
		stringstream sstream;
		sstream << i;
		sstream >> tempstr;
		const char* name = tempstr.c_str();
		iofDatapage* pagetemp = (iofDatapage*)pmem_map_file(name, sizeof(iofDatapage), PMEM_FILE_CREATE, 0777, &psize, &pmd);
		data_page inpage;
		inpage.fileid = pagetemp->fileid;
		for(int j = 0;j<DATA_PAGE_SLOT_NUM;++j){
			pm_bucket temp_b;
			inpage.slot.push_back(temp_b);
			inpage.bitmap[j] = pagetemp->bitmap[j];
			inpage.slot[j].bitmap[0] = pagetemp->slot[j].bitmap[0];
			inpage.slot[j].bitmap[1] = pagetemp->slot[j].bitmap[1];
			inpage.slot[j].local_depth = pagetemp->slot[j].local_depth;
			for (int k = 0; k < BUCKET_SLOT_NUM; ++k) {
				inpage.slot[j].slot[k].key = pagetemp->slot[j].slot[k].key;
				inpage.slot[j].slot[k].value = pagetemp->slot[j].slot[k].value;
			}
		}
		datapage.push_back(inpage);
		pmem_unmap(pagetemp, psize);
	}
	mapAllPage();
}

/**
 * @description: make sure each virtual address for the pages
 * @param NULL
 * @return: NULL
 */
void PmEHash::mapAllPage() {
	for (int i = 0; i < metadata->catalog_size; ++i)//ergodic each catalog
		if(catalog.buckets_pm_address[i].fileId !=0){
			pm_bucket * temp  = &datapage[0].slot.front(); //get the beginning address
			catalog.buckets_virtual_address[i] = temp + catalog.buckets_pm_address[i].offset/256;
		}
}

/**
 * @description: 
 * @param NULL
 * @return: NULL
 */
void PmEHash::selfDestory() {

}

