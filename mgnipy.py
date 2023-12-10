import argparse
import requests

class GenomeData:
    def __init__(self, id, attributes):
        self.id = id
        self.attributes = attributes

class GenomeAttributes:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key.replace('-', '_'), value)  # Update attribute names here

def genome_search(args):
    base_url = "https://www.ebi.ac.uk/metagenomics/api/v1/genomes"
    response = requests.get(base_url)
    genome_data = response.json()

    data = genome_data['data']
    filter_genomes = []
    for genome in data:
        attributes = GenomeAttributes(**genome['attributes'])
        # Ensure attribute names match argument names
        if all(
            (getattr(attributes, attr) == getattr(args, attr.replace('-', '_')))
            if not attr.startswith('gc_content')  # Skip direct comparisons for gc-content
            else (
                getattr(attributes, 'gc_content') is not None
                and (args.gc_content_min or 0) <= getattr(attributes, 'gc_content') <= (args.gc_content_max or 100)
            )
            for attr in vars(args) if getattr(args, attr) is not None
        ):
            filter_genomes.append(GenomeData(genome['id'], attributes))

    for genome in filter_genomes:
        print(f"Genome ID: {genome.id}")
        print(f"Genome Accession: {genome.attributes.accession}")
        print(f"Genome Length: {genome.attributes.length}")
        print(f"Geographic Location: {genome.attributes.geographic_origin}")
        print(f"GC-Content: {genome.attributes.gc_content}\n")

def main():
    parser = argparse.ArgumentParser(description='Search genomes on MGnify API')
    parser.add_argument('--geographic-origin', help='Geographic origin')
    parser.add_argument('--accession', help='Accession ID')
    parser.add_argument('--genome-type', help='Type of genome')
    parser.add_argument('--length', type=int, help='Genome length')
    parser.add_argument('--gc-content-min', type=float, help='Minimum GC content')
    parser.add_argument('--gc-content-max', type=float, help='Maximum GC content')

    # Add more arguments for other attributes

    args = parser.parse_args()
    genome_search(args)

if __name__ == "__main__":
    main()






































