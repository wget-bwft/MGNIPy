import argparse
import csv
import asyncio
import aiohttp

class GenomeData:
    def __init__(self, id, attributes, biome=None):
        self.id = id
        self.attributes = attributes
        self.biome = biome

class GenomeAttributes:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key.replace('-', '_'), value)

async def fetch_genomes_async(session, page_num, filters=None):
    base_url = f"https://www.ebi.ac.uk/metagenomics/api/v1/genomes?page={page_num}"
    
    if filters:
        base_url += '&' if '?' in base_url else '?'
        base_url += '&'.join([f"{key}={value}" for key, value in filters.items()])

    async with session.get(base_url) as response:
        return await response.json()

async def fetch_biome_data(session, url):
    async with session.get(url) as response:
        return await response.json()

async def fetch_all_genomes(args, filters):
    MAX_PAGES = 10
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_genomes_async(session, page_num, filters) for page_num in range(1, MAX_PAGES + 1)]
        genome_data = await asyncio.gather(*tasks)

        filtered_genomes = []
        for page_data in genome_data:
            if 'data' in page_data and page_data['data']:
                for genome in page_data['data']:
                    attributes = GenomeAttributes(**genome['attributes'])

                    if args.geographic_origin and attributes.geographic_origin != args.geographic_origin:
                        continue

                    if args.gc_content_max and attributes.gc_content > args.gc_content_max:
                        continue

                    if args.length and attributes.length > args.length:
                        continue

                    if args.num_contigs and attributes.num_contigs > args.num_contigs:
                        continue

                    if args.completeness and attributes.completeness < args.completeness:
                        continue

                    # Add similar checks for other attributes...

                    if 'relationships' in genome and 'biome' in genome['relationships']:
                        biome_url = genome['relationships']['biome']['links']['related']
                        biome_info = await fetch_biome_data(session, biome_url)
                        biome_data = biome_info['data'] if 'data' in biome_info else None
                    else:
                        biome_data = None

                    genome_obj = GenomeData(genome['id'], attributes, biome_data)
                    filtered_genomes.append(genome_obj)

        return filtered_genomes

def write_to_csv(data_list):
    with open('genomes_data.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'Genome ID', 'Geographic Origin', 'Accession', 'ENA Genome Accession', 'ENA Sample Accession',
            'ENA Study Accession', 'NCBI Genome Accession', 'NCBI Sample Accession', 'NCBI Study Accession',
            'IMG Genome Accession', 'PATRIC Genome Accession', 'Genome Length', 'Number of Contigs', 'N50',
            'GC Content', 'Genome Type', 'Completeness', 'Contamination', 'RNA 5S', 'RNA 16S', 'RNA 23S',
            'tRNAs', 'ncRNAs', 'Number of Proteins', 'EggNOG Coverage', 'InterPro Coverage', 'Taxon Lineage',
            'Number of Genomes Total', 'Pangenome Size', 'Pangenome Core Size', 'Pangenome Accessory Size'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for genome in data_list:
            writer.writerow({
                'Genome ID': genome.id,
                'Geographic Origin': genome.attributes.geographic_origin,
                'Accession': genome.attributes.accession,
                'ENA Genome Accession': genome.attributes.ena_genome_accession,
                'ENA Sample Accession': genome.attributes.ena_sample_accession,
                'ENA Study Accession': genome.attributes.ena_study_accession,
                'NCBI Genome Accession': genome.attributes.ncbi_genome_accession,
                'NCBI Sample Accession': genome.attributes.ncbi_sample_accession,
                'NCBI Study Accession': genome.attributes.ncbi_study_accession,
                'IMG Genome Accession': genome.attributes.img_genome_accession,
                'PATRIC Genome Accession': genome.attributes.patric_genome_accession,
                'Genome Length': genome.attributes.length,
                'Number of Contigs': genome.attributes.num_contigs,
                'N50': genome.attributes.n_50,
                'GC Content': genome.attributes.gc_content,
                'Genome Type': genome.attributes.type,
                'Completeness': genome.attributes.completeness,
                'Contamination': genome.attributes.contamination,
                'RNA 5S': genome.attributes.rna_5s,
                'RNA 16S': genome.attributes.rna_16s,
                'RNA 23S': genome.attributes.rna_23s,
                'tRNAs': genome.attributes.trnas,
                'ncRNAs': genome.attributes.nc_rnas,
                'Number of Proteins': genome.attributes.num_proteins,
                'EggNOG Coverage': genome.attributes.eggnog_coverage,
                'InterPro Coverage': genome.attributes.ipr_coverage,
                'Taxon Lineage': genome.attributes.taxon_lineage,
                'Number of Genomes Total': genome.attributes.num_genomes_total,
                'Pangenome Size': genome.attributes.pangenome_size,
                'Pangenome Core Size': genome.attributes.pangenome_core_size,
                'Pangenome Accessory Size': genome.attributes.pangenome_accessory_size
            })

async def genome_search_async(args):
    filters = {
        'geographic_origin': args.geographic_origin,
        'gc_content_max': args.gc_content_max,
        'length': args.length,
        'num_contigs': args.num_contigs,
        'completeness': args.completeness
        # Add other filters here...
    }

    filters = {key: value for key, value in filters.items() if value is not None}

    genomes = await fetch_all_genomes(args, filters)
    write_to_csv(genomes)

async def main_async(args):
    await genome_search_async(args)

if __name__ == "__main__":
    usage = '''%(prog)s [--geographic-origin GEOGRAPHIC_ORIGIN] [--gc-content-max GC_CONTENT_MAX]
               [--length LENGTH] [--num-contigs NUM_CONTIGS] [--completeness COMPLETENESS]
               # Add other arguments here...
               '''

    parser = argparse.ArgumentParser(description='Search genomes on MGnify API', usage=usage)
    parser.add_argument('--geographic-origin', help='Geographic origin')
    parser.add_argument('--gc-content-max', type=float, help='Maximum GC content')
    parser.add_argument('--length', type=int, help='Maximum genome length')
    parser.add_argument('--num-contigs', type=int, help='Maximum number of contigs')
    parser.add_argument('--completeness', type=float, help='Minimum completeness')

    # Add other argument parsers for additional attributes...
    # ...

    args = parser.parse_args()
    asyncio.run(main_async(args))




















































