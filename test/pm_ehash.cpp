#include"pm_ehash.h"
#include <utility>
#include <cmath>
#include <fstream>
#include <string>
#include <iostream>
#include <sstream>
/**
 * @description: construct default for PmHash 
 * @param NULL
 * @return: a initial instance of PmEHash
 */
PmEHash::PmEHash() {
	metadata = new ehash_metadata;
	metadata->sum_of_bucket = 16;
	metadata->catalog_size = 16;
	metadata->global_depth = 4;
	metadata->max_file_id = 2;
	data_page Datapage;
	Datapage.fileid = 1;
	for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i)
		Datapage.bitmap[i] = 0;	//initialize Datapage bitmap with 0
		
	for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i) {
		pm_bucket test;
		test.local_depth = 4; //Each bucket store less than 2^4
		Datapage.slot.push_back(test); //push default 32 buckets 
	}
	datapage.push_back(Datapage); //push a defaul Datapage into datapage
	
	for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i) {
		datapage[0].slot[i].bitmap[0] = 0;
		datapage[0].slot[i].bitmap[1] = 0; //initialize two size8 bitmap with 0
	}
	catalog.buckets_virtual_address = new pm_bucket * [DEFAULT_CATALOG_SIZE];
	
	for (int i = 0; i < DEFAULT_CATALOG_SIZE; ++i) 
		catalog.buckets_virtual_address[i] = &datapage[0].slot[i]; //set virtual address
	
	catalog.buckets_pm_address = new pm_address[DEFAULT_CATALOG_SIZE];// set pm address
	
	for (int i = 0; i < DEFAULT_CATALOG_SIZE; ++i) {
		catalog.buckets_pm_address[i].fileId = Datapage.fileid;
		catalog.buckets_pm_address[i].offset = i * sizeof(pm_bucket);
	}

	for (int i = DEFAULT_CATALOG_SIZE; i < DATA_PAGE_SLOT_NUM; ++i) 
		free_list.push(&datapage[0].slot[i]);

	for (int i = 0; i < DEFAULT_CATALOG_SIZE; ++i) {
		vAddr2pmAddr.insert(pair<pm_bucket*, pm_address>(&datapage[0].slot[i], catalog.buckets_pm_address[i]));
		pmAddr2vAddr.insert(pair<pm_address, pm_bucket*>(catalog.buckets_pm_address[i], &datapage[0].slot[i]));
	}

}
/**
 * @description: Deconstruct function for PmHash
 * @param NULL
 * @return: NULL
 */
PmEHash::~PmEHash() {
	//������ַ  -- �����ַ
	/*ofstream file_Vtopm;
	file_Vtopm.open("Vtopm.txt");
	for (int i = 0; i < vAddr2pmAddr.size(); ++i) {
		file_Vtopm << catalog.buckets_pm_address[i].fileId << " " << catalog.buckets_pm_address[i].offset << " " << vAddr2pmAddr[i] << endl;
	}

	ofstream file_pmtoV;
	file_pmtoV.open("pmtoV.txt");
	for (int i = 0; i < pmAddr2vAddr.size(); ++i) {
		file_Vtopm << catalog.buckets_pm_address[i].fileId << " " << catalog.buckets_pm_address[i].offset << " " << pmAddr2vAddr[i] << endl;
	}*/


	//catalog
	/*ofstream file_catalog;
	file_catalog.open("catalog.txt");
	file_catalog << metadata->catalog_size<<endl;
	for (int i = 0; i < metadata->catalog_size; ++i) {
		file_catalog << catalog.buckets_pm_address[i].fileId<<" ";
		file_catalog << catalog.buckets_pm_address[i].offset << endl;
	}
	file_catalog.close();

	//metadata
	ofstream file_metadata;
	file_metadata.open("metadata.txt");
	file_metadata << metadata->max_file_id << endl;
	file_metadata << metadata->catalog_size << endl;
	file_metadata << metadata->global_depth<<endl;
	file_metadata.close();

	//datapage
	for (int i = 1; i <= datapage.size() ; ++i) {
		ofstream file_page;
		string temp_s;
		stringstream stream;
		stream << i;
		stream << ".txt";
		stream >> temp_s;
		file_page.open(temp_s);
		file_page << datapage[i].fileid << endl;
		for (int t = 0; t < 32; ++t) {
			file_page << datapage[i].bitmap[t];
		}
		file_page << endl;
		for (int j = 0; j < datapage[i].slot.size() && datapage[i].bitmap[j] != 0; ++j) {
			file_page << datapage[i].slot[j].bitmap[0] << datapage[i].slot[j].bitmap[0] << endl;
			int check[16];
			uint8_t  test1 = datapage[i].slot[j].bitmap[0];
			uint8_t test2 = datapage[i].slot[j].bitmap[1];
			for (int k = 0; k < 8; ++k) {
				check[k] = test2 % 2;
				test2 = test2 / 2;
			}
			for (int k = 8; k < 16; ++k) {
				check[k] = test1 % 2;
				test1 = test1 / 2;
			}
			for (int k = 15; k > 8; --k) {
				if (check[k] == 1) {
					//return &bucket->slot[15 - i];
					file_page << datapage[i].slot[j].slot[15-k].key<<" ";
					file_page << datapage[i].slot[j].slot[15-k].value<<endl;

				}
			}
			for (int k = 7; k >= 0; --k) {
				if (check[i] == 1) {
					//return &bucket->slot[15 - i];
					file_page << datapage[i].slot[j].slot[15 - k].key << " ";
					file_page << datapage[i].slot[j].slot[15 - k].value << endl;
				}
			}
		}
		file_page.close();
	}*/

}

/**
 * @description: insert a new kv , and update the correspondedding bitmap to 1
 * @param kv: an new kv
 * @return: return 0 for success
 			return -1 for already existing
 */
int PmEHash::insert(kv new_kv_pair) {
	uint64_t value;
	if (search(new_kv_pair.key, value) == 0) return -1; //exist 
	//no exist then insert
	pm_bucket* bucket = getFreeBucket(new_kv_pair.key);
	kv* freePlace = getFreeKvSlot(bucket);
	*freePlace = new_kv_pair;
	
	int position = (freePlace - bucket->slot);
	if (position <= 7) bucket->bitmap[0] += pow(2, 7 - position);
	else
		bucket->bitmap[1] += pow(2, 15 - position);//bitmap to 1
	return 0;
}

/**
 * @description: delete a kv and update the correspondding bitmap to 0
 * @param uint64_t: the kv key to be deleted
 * @return: return 0 for success removement
 			return -1 for fail as no existing before
 */
int PmEHash::remove(uint64_t key) {
	uint64_t bucket_id = hashFunc(key);
	pm_bucket *bucket = catalog.buckets_virtual_address[bucket_id]; 
	
	//find data's position
	int bits;
	int number;
	bits = bucket->bitmap[1] > 0 ? 16 : 8;
	number = bucket->bitmap[1] > 0 ? bucket->bitmap[1] : bucket->bitmap[0];
	if (number == 0 && bits == 8) bits = 0;
	else if (number == 0 && bits == 16) bits = 8;
	else {
		while (number % 2 != 1) {
			bits--;
			number /= 2;
		}
	}
	int position = -1;
	for (int i = 0; i < bits; i++) {
		uint8_t num = (i <= 7 ? bucket->bitmap[0] : bucket->bitmap[1]);
		int b = (i <= 7 ? 7 : 15);	
		if ((num >> (b-i))%2 == 1 && bucket->slot[i].key == key) {
			position = i;
			break;
		}
	}	
	
	//NOT found
	if (position == -1) return -1; 	
	
	// bitmap to 0
	if (position <= 7) bucket->bitmap[0] -= pow(2, 7 - position);
	else
		bucket->bitmap[0] -= pow(2, 15 - position);
	if (bucket->bitmap[0] == 0 && bucket->bitmap[1] == 0) mergeBucket(bucket_id);
	return 0;
}
/**
 * @description: update the value for an already existing kv's key
 * @param kv: a new kv, same key but different value
 * @return: return 0 for success removement
 			return -1 for fail as no existing before
 */
int PmEHash::update(kv kv_pair) {
	uint64_t bucket_id = hashFunc(kv_pair.key);
	pm_bucket *bucket = catalog.buckets_virtual_address[bucket_id]; 
	
	//num of data in slot
	int bits;
	int number;    
	bits = bucket->bitmap[1] > 0 ? 16 : 8;
	number = bucket->bitmap[1] > 0 ? bucket->bitmap[1] : bucket->bitmap[0];
	if (number == 0 && bits == 8) bits = 0;
	else if (number == 0 && bits == 16) bits = 8;
	else {
		while (number % 2 != 1) {
			bits--;
			number /= 2;
		}
	}
	
	//fix the position and updata the value    
	for (int i = 0; i < bits; i++) {
		uint8_t num = (i <= 7 ? bucket->bitmap[0] : bucket->bitmap[1]);
		int b = (i <= 7 ? 7 : 15);
		if ((num >> (b-i))%2 == 1 && bucket->slot[i].key == kv_pair.key) {
			bucket->slot[i].value = kv_pair.value;
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
	uint64_t bucket_id = hashFunc(key);
	pm_bucket *bucket = catalog.buckets_virtual_address[bucket_id]; 
	
	//num of kv in the slot
	int bits;
	int number;  
	bits = bucket->bitmap[1] > 0 ? 16 : 8;
	number = bucket->bitmap[1] > 0 ? bucket->bitmap[1] : bucket->bitmap[0];
	if (number == 0 && bits == 8) bits = 0;
	else if (number == 0 && bits == 16) bits = 8;
	else {
		while (number % 2 != 1) {
			bits--;
			number /= 2;
		}
	}
	
	//ergodic and make it out
	for (int i = 0; i < bits; i++) {
		uint8_t num = (i <= 7 ? bucket->bitmap[0] : bucket->bitmap[1]);
		int b = (i <= 7 ? 7 : 15);
		if ((num >> (b-i))%2 == 1 && bucket->slot[i].key == key) {
			return_val = bucket->slot[i].value;
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
	int tmp;
	int pos;
	for (int i = 4; i <= metadata->global_depth; ++i) {
		tmp = (1 << i);
		pos = key % tmp;
		if (catalog.buckets_virtual_address[pos]->local_depth == i)return pos;
	}
}

/**
 * @description: with the key, find the target order of bucket, eixst then return its virtual address, or split and return a new bucket's virtual address
 * @param uint64_t: a kv's key
 * @return: virtual address of the target bucket
 */
pm_bucket* PmEHash::getFreeBucket(uint64_t key) {
	uint64_t Key = hashFunc(key);
	pm_bucket* temp_bucket = catalog.buckets_virtual_address[Key];
	while ((*temp_bucket).bitmap[0] == 255 && (*temp_bucket).bitmap[1] == 254) {
		splitBucket(Key);
		uint64_t temp = 1;
		temp = (temp << (temp_bucket->local_depth-1));
		temp_bucket = catalog.buckets_virtual_address[Key];
	}
	Key = hashFunc(key); 
	return catalog.buckets_virtual_address[Key];

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
	//num of kv in the slot
	int bits;
	int number;
	bits = bucket->bitmap[1] > 0 ? 16 : 8;
	number = bucket->bitmap[1] > 0 ? bucket->bitmap[1] : bucket->bitmap[0];
	if (number == 0 && bits == 8) return &bucket->slot[0];
	if (number == 0 && bits == 16) return &bucket->slot[8];
	while (number % 2 != 1) {
		bits--;
		number = number / 2;
	}
	for (int i = 0; i < bits; i ++) {
		uint8_t num = (i <= 7 ? bucket->bitmap[0] : bucket->bitmap[1]);
		int b = (i <= 7 ? 7 : 15);
		if ((num >> (b-i))%2 == 0) {
			return &bucket->slot[i]; 
		}
	}
	return &bucket->slot[bits];
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
	uint64_t page_num = bucket_id + num;//order of the new bucket 
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
	//same local_depth
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
	if (bucket_id >= 0 && bucket_id <= 15) return;
	else {
		int position;
		pm_bucket *bucket = catalog.buckets_virtual_address[bucket_id]; 
		
		bool status = false; //A bucket have splited or not
		uint64_t depth = bucket->local_depth;
		while(1) {
			if (bucket_id >= pow(2,depth-1) && bucket_id < pow(2,depth)) break;
			depth--;
			status = true;
		}
		//have Splited
		if (status) {
			uint64_t next_bucket_id = bucket_id + pow(2,bucket->local_depth-1);
			pm_bucket *next_bucket = catalog.buckets_virtual_address[next_bucket_id];
			bucket->local_depth--;
			// num of kv in slot
			int bits;
			int number;    
			bits = next_bucket->bitmap[1] > 0 ? 16 : 8;
			number = next_bucket->bitmap[1] > 0 ? next_bucket->bitmap[1] : next_bucket->bitmap[0];
			if (number == 0 && bits == 8) bits = 0;
			else if (number == 0 && bits == 16) bits = 8;
			else {
				while (number % 2 != 1) {
					bits--;
					number /= 2;
				}
			}
			//kv in splited bucket again into the basic bucket
			for (int i = 0; i < bits; i++) {
				uint8_t num = (i <= 7 ? next_bucket->bitmap[0] : next_bucket->bitmap[1]);
				int b = (i <= 7 ? 7 : 15);
				if ((num >> (b-i))%2 == 1) {
					insert(next_bucket->slot[i]);
				}
			}
			//set the next bucket's bitmap to 0
			next_bucket->bitmap[0] = next_bucket->bitmap[1] = 0;
			catalog.buckets_virtual_address[next_bucket_id] = NULL;
			position = catalog.buckets_pm_address[next_bucket_id].offset / sizeof(pm_bucket); 
		}
	
		//never splited
		else {
			uint64_t last_bucket_id = bucket_id - pow(2,bucket->local_depth-1);
			pm_bucket *last_bucket = catalog.buckets_virtual_address[last_bucket_id];
			//same local_depth, merge
			if (bucket->local_depth == last_bucket->local_depth) {
				last_bucket->local_depth--;
				catalog.buckets_virtual_address[bucket_id] = NULL;
				position = catalog.buckets_pm_address[bucket_id].offset / sizeof(pm_bucket);
			}
			else
				position = -1;//diff local_depth, keep
		}
		//ɾ��ӳ�����Ͱ�ĵ�ַ
		//pmAddr2vAddr.erase(it);
		
		//on Datapage, delete the bucket
		if (position != -1) {
			free_list.push(&datapage[catalog.buckets_pm_address[bucket_id].fileId - 1].slot[position]);
			//λͼ��0
			datapage[catalog.buckets_pm_address[bucket_id].fileId - 1].bitmap[position] = 0;
			metadata->sum_of_bucket--;
		}
		
	}
}
/**
 * @description: ��Ŀ¼���б�������Ҫ���������µ�Ŀ¼�ļ������ƾ�ֵ��Ȼ��ɾ���ɵ�Ŀ¼�ļ�
 * @param NULL
 * @return: NULL
 */
void PmEHash::extendCatalog() {//
	int size = metadata->catalog_size * 2;
	ehash_catalog new_catalog;
	new_catalog.buckets_pm_address = new pm_address[size];
	new_catalog.buckets_virtual_address = new pm_bucket * [size];
	//����Ŀ¼�����ݿ�����ȥ
	for (int i = 0; i < size / 2; i++) {
		new_catalog.buckets_pm_address[i] = catalog.buckets_pm_address[i];
		new_catalog.buckets_virtual_address[i] = catalog.buckets_virtual_address[i];
	}
	for (int i = size/2; i < size; i++)
		new_catalog.buckets_virtual_address[i] = NULL;
	//ɾ����Ŀ¼
	delete[]catalog.buckets_pm_address;
	delete[]catalog.buckets_virtual_address;
	catalog = new_catalog;
	//����metadata 
	metadata->global_depth++;
	metadata->catalog_size *= 2;
}

/**
 * @description: ���һ�����õ�����ҳ���²�λ����ϣͰʹ�ã����û�����������µ�����ҳ
 * @param pm_address&: �²�λ�ĳ־û��ļ���ַ����Ϊ���ò�������
 * @return: �²�λ�������ַ
 */
void* PmEHash::getFreeSlot(pm_address& new_address) {
	if (free_list.size() == 0) {
		allocNewPage();
	}

	pm_bucket* temp = free_list.front();
	free_list.pop();
	return temp;
}

/**
 * @description: �����µ�����ҳ�ļ������������²����Ŀ��в۵ĵ�ַ����free_list�����ݽṹ��
 * @param NULL
 * @return: NULL
 */
void PmEHash::allocNewPage() {
	data_page temp_page;
	temp_page.fileid = datapage.size() + 1;
	datapage.push_back(temp_page);
	for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i) {
		datapage[datapage.size() - 1].bitmap[i] = 0;
	}
	for (int i = 0; i < 32; ++i) {
		pm_bucket temp_bucket;
		temp_bucket.bitmap[0] = 0;
		temp_bucket.bitmap[1] = 0;
		datapage[datapage.size() - 1].slot.push_back(temp_bucket);
	}
	for (int i = 0; i < 32; ++i) {
		free_list.push(&(datapage[datapage.size() - 1].slot[i]));
	}
	metadata->max_file_id++;
}

/**
 * @description: ��ȡ�������ļ����������ϣ���ָ���ϣ�ر�ǰ��״̬
 * @param NULL
 * @return: NULL
 */
void PmEHash::recover() {
	//��metadata��
	/*metadata = new ehash_metadata;
	ifstream file_metadata;
	file_metadata.open("metadata.txt");
	uint64_t max_file_id;
	file_metadata >> metadata->max_file_id;
	file_metadata >> metadata->catalog_size;
	file_metadata >> metadata->global_depth;
	file_metadata.close();

	//��catalog
	int size;
	ifstream file_catalog;
	file_catalog.open("catalog.txt");
	file_catalog >> size;
	catalog.buckets_virtual_address = new pm_bucket * [size];
	catalog.buckets_pm_address = new pm_address  [size];
	for (int i = 0; i < size; ++i) {
		file_catalog >> catalog.buckets_pm_address[i].fileId;

	}*/

}

/**
 * @description: ����ʱ������������ҳ�����ڴ�ӳ�䣬���õ�ַ���ӳ���ϵ�����еĺ�ʹ�õĲ�λ����Ҫ����
 * @param NULL
 * @return: NULL
 */
void PmEHash::mapAllPage() {

}

/**
 * @description: ɾ��PmEHash������������ҳ��Ŀ¼��Ԫ�����ļ�����Ҫ��gtestʹ�á���������п���չ��ϣ���ļ����ݣ���ֹ���ڴ��ϵ�
 * @param NULL
 * @return: NULL
 */
void PmEHash::selfDestory() {

}

int main() {
	PmEHash A;
	for (int i = 0; i < 32; ++i) {
		kv temp;
		temp.key = i*16;
		temp.value = i*16;
		A.insert(temp);
	}
	for (int i = 0; i < 32; ++i) {
		uint64_t key = i*16;
		if (key%64 == 0) continue; 
		A.remove(key);
	}
	for (int i = 0; i < 22; ++i) {
		for (int j = 0; j < 15; ++j) {
			cout << A.datapage[0].slot[i].slot[j].value << " ";
		}
		cout<< A.datapage[0].slot[i].local_depth;
		cout << endl;
	}
	for (int i = 0; i < 22; ++i) {
		cout << i << " ";
		for (int j = 0; j < 2; ++j) {
			cout << (int)A.datapage[0].slot[i].bitmap[j] << " ";
		}
		cout << endl;
	}
	cout << endl;
}
 
