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
    async with aiohttp.ClientSession() as session:
        page_num = 1
        filtered_genomes = []

        while True:  # Continue fetching until there's no more data
            base_url = f"https://www.ebi.ac.uk/metagenomics/api/v1/genomes?page={page_num}"

            if filters:
                base_url += '&' if '?' in base_url else '?'
                base_url += '&'.join([f"{key}={value}" for key, value in filters.items()])

            async with session.get(base_url) as response:
                page_data = await response.json()

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

                        if args.type and attributes.type != args.type:
                            continue

                        if args.contamination and attributes.contamination > args.contamination:
                            continue

                        if args.genome_id and attributes.genome_id != args.genome_id:
                            continue

                        if args.ena_genome_accession and attributes.ena_genome_accession != args.ena_genome_accession:
                            continue

                        if args.ena_sample_accession and attributes.ena_sample_accession != args.ena_sample_accession:
                            continue

                        if args.ena_study_accession and attributes.ena_study_accession != args.ena_study_accession:
                            continue

                        if args.ncbi_genome_accession and attributes.ncbi_genome_accession != args.ncbi_genome_accession:
                            continue

                        if args.ncbi_sample_accession and attributes.ncbi_sample_accession != args.ncbi_sample_accession:
                            continue

                        if args.ncbi_study_accession and attributes.ncbi_study_accession != args.ncbi_study_accession:
                            continue

                        if args.img_genome_accession and attributes.img_genome_accession != args.img_genome_accession:
                            continue

                        if args.patric_genome_accession and attributes.patric_genome_accession != args.patric_genome_accession:
                            continue

                        if args.n_50 and attributes.n_50 > args.n_50:
                            continue

                        if args.gc_content_min and attributes.gc_content < args.gc_content_min:
                            continue

                        if args.rna_5s and attributes.rna_5s != args.rna_5s:
                            continue

                        if args.rna_16s and attributes.rna_16s != args.rna_16s:
                            continue

                        if args.rna_23s and attributes.rna_23s != args.rna_23s:
                            continue

                        if args.trnas and attributes.trnas != args.trnas:
                            continue

                        if args.nc_rnas and attributes.nc_rnas != args.nc_rnas:
                            continue

                        if args.num_proteins and attributes.num_proteins != args.num_proteins:
                            continue

                        if args.eggnog_coverage and attributes.eggnog_coverage != args.eggnog_coverage:
                            continue

                        if args.ipr_coverage and attributes.ipr_coverage != args.ipr_coverage:
                            continue

                        if args.taxon_lineage and attributes.taxon_lineage != args.taxon_lineage:
                            continue

                        if args.num_genomes_total and attributes.num_genomes_total != args.num_genomes_total:
                            continue

                        if args.pangenome_size and attributes.pangenome_size != args.pangenome_size:
                            continue

                        if args.pangenome_core_size and attributes.pangenome_core_size != args.pangenome_core_size:
                            continue

                        if args.pangenome_accessory_size and attributes.pangenome_accessory_size != args.pangenome_accessory_size:
                            continue

                        # Append Biome data if available
                        if 'relationships' in genome and 'biome' in genome['relationships']:
                            biome_url = genome['relationships']['biome']['links']['related']
                            biome_info = await fetch_biome_data(session, biome_url)
                            biome_data = biome_info['data'] if 'data' in biome_info else None
                        else:
                            biome_data = None

                        genome_obj = GenomeData(genome['id'], attributes, biome_data)
                        filtered_genomes.append(genome_obj)

                    # Move to the next page
                    page_num += 1
                else:
                    break  # Break the loop if no more data

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
        'genome_id': args.genome_id,
        'ena_genome_accession': args.ena_genome_accession,
        'ena_sample_accession': args.ena_sample_accession,
        'ena_study_accession': args.ena_study_accession,
        'ncbi_genome_accession': args.ncbi_genome_accession,
        'ncbi_sample_accession': args.ncbi_sample_accession,
        'ncbi_study_accession': args.ncbi_study_accession,
        'img_genome_acession': args.img_genome_accession,
        'patric_genome_acession': args.patric_genome_accession,
        'length': args.length,
        'num_contigs': args.num_contigs,
        'n_50': args.n_50,
        'gc_content_max': args.gc_content_max,
        'gc_content_min': args.gc_content_min,
        'type': args.type,
        'completeness': args.completeness,
        'contamination': args.contamination,
        'rna_5s': args.rna_5s,
        'rna_16s': args.rna_16s,
        'rna_23s': args.rna_23s,
        'trnas': args.trnas,
        'nc_rnas': args.nc_rnas,
        'num_proteins': args.num_proteins,
        'eggnog_coverage': args.eggnog_coverage,
        'ipr_coverage': args.ipr_coverage,
        'taxon_lineage': args.taxon_lineage,
        'num_genomes_total': args.num_genomes_total,
        'pangenome_size': args.pangenome_size,
        'pangenome_core_size': args.pangenome_core_size,
        'pangenome_accessory_size': args.pangenome_accessory_size
    }

    filters = {key: value for key, value in filters.items() if value is not None}

    genomes = await fetch_all_genomes(args, filters)
    write_to_csv(genomes)

async def main_async(args):
    await genome_search_async(args)

if __name__ == "__main__":
    usage = '''%(prog)s [--geographic-origin GEOGRAPHIC_ORIGIN] [--gc-content-max GC_CONTENT_MAX]
               [--length LENGTH] [--num-contigs NUM_CONTIGS] [--completeness COMPLETENESS]
               [--type TYPE] [--contamination CONTAMINATION] [--genome-id GENOME_ID]
               [--ena-genome-accession ENA_GENOME_ACCESSION] [--ena-sample-accession ENA_SAMPLE_ACCESSION]
               [--ena-study-accession ENA_STUDY_ACCESSION] [--ncbi-genome-accession NCBI_GENOME_ACCESSION]
               [--ncbi-sample-accession NCBI_SAMPLE_ACCESSION] [--ncbi-study-accession NCBI_STUDY_ACCESSION]
               [--img-genome-accession IMG_GENOME_ACCESSION] [--patric-genome-accession PATRIC_GENOME_ACCESSION]
               [--n-50 N_50] [--gc-content-min GC_CONTENT_MIN] [--rna-5s RNA_5S] [--rna-16s RNA_16S]
               [--rna-23s RNA_23S] [--trnas TRNAS] [--nc-rnas NC_RNAS] [--eggnog-coverage EGGNOG_COVERAGE]
               [--ipr-coverage IPR_COVERAGE] [--taxon-lineage TAXON_LINEAGE] [--num-genomes-total NUM_GENOMES_TOTAL]
               [--pangenome-size PANGENOME_SIZE] [--pangenome-core-size PANGENOME_CORE_SIZE]
               [--pangenome-accessory-size PANGENOME_ACCESSORY_SIZE] [--num-proteins NUM_PROTEINS]
               '''

    parser = argparse.ArgumentParser(description='Search genomes on MGnify API', usage=usage)
    parser.add_argument('--geographic-origin', help='Geographic origin')
    parser.add_argument('--gc-content-max', type=float, help='Maximum GC content')
    parser.add_argument('--length', type=int, help='Maximum genome length')
    parser.add_argument('--num-contigs', type=int, help='Maximum number of contigs')
    parser.add_argument('--completeness', type=float, help='Minimum completeness')
    parser.add_argument('--type', type=str, help='Genome type')
    parser.add_argument('--contamination', type=float, help='Maximum contamination')

    # Add other argument parsers for additional attributes...
    parser.add_argument('--genome-id', type=str, help='Genome ID')
    parser.add_argument('--ena-genome-accession', type=str, help='ENA Genome Accession')
    parser.add_argument('--ena-sample-accession', type=str, help='ENA Sample Accession')
    parser.add_argument('--ena-study-accession', type=str, help='ENA Study Accession')
    parser.add_argument('--ncbi-genome-accession', type=str, help='NCBI Genome Accession')
    parser.add_argument('--ncbi-sample-accession', type=str, help='NCBI Sample Accession')
    parser.add_argument('--ncbi-study-accession', type=str, help='NCBI Study Accession')
    parser.add_argument('--img-genome-accession', type=str, help='IMG Genome Accession')
    parser.add_argument('--patric-genome-accession', type=str, help='PATRIC Genome Accession')
    parser.add_argument('--n-50', type=int, help='N50')
    parser.add_argument('--gc-content-min', type=float, help='Minimum GC content')
    parser.add_argument('--rna-5s', type=int, help='RNA 5S')
    parser.add_argument('--rna-16s', type=int, help='RNA 16S')
    parser.add_argument('--rna-23s', type=int, help='RNA 23S')
    parser.add_argument('--trnas',type=float, help='tRNAs')
    parser.add_argument('--nc-rnas', type=float, help='Non-coding tRNAs')
    parser.add_argument('--eggnog-coverage', type=float, help='EggNOG Coverage')
    parser.add_argument('--ipr-coverage', type=float, help='InterPro Coverage')
    parser.add_argument('--taxon-lineage', type=str, help='Taxon Lineage')
    parser.add_argument('--num-genomes-total', type=int, help='Number of Genomes Total')
    parser.add_argument('--pangenome-size', type=int, help='Pangenome Size')
    parser.add_argument('--pangenome-core-size', type=int, help='Pangenome Core Size')
    parser.add_argument('--pangenome-accessory-size', type=int, help='Pangenome Accessory Size')
    parser.add_argument('--num-proteins', type=int, help='Number of Proteins')


    args = parser.parse_args()
    asyncio.run(main_async(args))




















































