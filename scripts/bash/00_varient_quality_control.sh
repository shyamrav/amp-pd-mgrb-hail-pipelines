#!/bin/bash

# 1. Variant-level QC:
#	a. Keep variants with
#.   i. Max missing genotype rate 10%
#.   ii. Bi-allelic
#.   iii. Minimum read depth ≥ 10X
#.   iv. Filter = PASS, genotype call quality ≥90
#	 The following conditions should be satisfied under PASS filter
		#.   v. Include SNPs: QD > 2.0, FS < 60, MQRankSum > -12.5, ReadPosRankSum > -8.0, SOR <= 3
		#.   Include inbreeding co-efficient > -0.8
		#.   vi. Include Indels: QD > 2.0, FS < 200, ReadPosRankSum > -20.0, SOR < 10.0
		#	does not violate hardy weinberg equilibrium
		#.  b. Annotate variants with VEP or snpEff
		#.   i. Annotate variants: snpEff or VEP
		#.   ii. Primary focus: coding variants
		#.   	- Keep categories: Missense, synonymous, splice donor, splice acceptor, stop gained, stop lost, start lost, frameshift, in-frame indels, and splice region variants
		# 	 	- Use only canonical transcripts for annotation
		#		- Damaging: splice donor, splice acceptor, stop gained, stop lost, start lost, frameshift, in-frame indels, and splice region variants, missense that are predicted as deleterious (SIFT) or P or D (PolyPhen2)
		# 2. Sample-level QC:
		#	a. PCA or Admixture analysis to select individuals of single ancestry
		#   b. Filter samples with second-degree relatedness or higher, where one of each pair of samples with a kinship coefficient of > 0.0884 can be removed.
		#   c. If Y chromosome data is available, infer sex using the Hail function impute_sex. This function should be performed on common biallelic SNPs (AF > 0.05) with a high callrate (callrate > 0.97). Remove samples with ambiguous sex and samples that are aneuploids.
		#   d. Outlier detection: sample with metric > mean + 3*stddev or metric < mean ? 3*stddev
		#   i. metrics to consider:
		#   	1. Expected ref/alt Ts/Tv ratio: WES ~3.0-3.3, WGS ~2.0-2.1.
		#   	2. Expected ancestral vs derived Ts/Tv ratio: WES ~1.1-1.2
		# 3. per haplotype SNVs (should be consistent across cases and controls)
		# 4. % known SNPs in CpG vs non-CpG regions should be consistent
		# 5. Ts/Tv ratio in known vs novel SNPs: Slightly higher Ts/Tv in known SNPs than novel SNPs
		# 6. Ts/Tv ratio in Synonymous/Missense mutations: Syn ~ 5.5, Mis ~ 2.2
		# 7. Distribution of doubletons, tripletons in cases and controls should follow exact binomial distribution (sometimes singletons can be exceptions)
		# 8 Indel ratio: common ~1; rare: 0.2 ? 0.5

# Binary Definition
SNPEFF_BIN=snpEff.jar
SNPEFF_DATABASE=GRCh37.75 #GRCh38.86
SNPSIFT_BIN=SnpSift.jar
SNPSIFT_DATABASE=dbNSFP4.0a.txt.gz #This version uses hg38. If hg19 reference was used in the vcf, this database needs to be adjusted accordingly

# Input Definition
INPUT_VCF_GZ=input.vcf.gz
QC_VCF_GZ=/qc.vcf.gz
SNPEFF_ANNOTATED_VCF=qc_snpeff.vcf
SNPSIFT_ANNOTATED_VCF=qc_snpeff_snpsift.vcf

# QC Filters
vcftools --gzvcf $INPUT_VCF_GZ \
   --max-missing 0.9 \
   --min-alleles 2 \
   --max-alleles 2 \
   --minDP 10 \
   --remove-filtered-all \
   --minQ 90 \
   --hwe 0.001 \
   --recode --recode-INFO-all --stdout | bgzip -c > $QC_VCF_GZ


# Annotation with SnpEff and SnpSift
java -Xmx40g -jar $SNPEFF_BIN $SNPEFF_DATABASE $QC_VCF_GZ > $SNPEFF_ANNOTATED_VCF
java -Xmx40g -jar $SNPSIFT_BIN dbnsfp -v -db $SNPSIFT_DATABASE \
-f hg19_chr,"hg19_pos(1-based)",aaref,aaalt,aapos,genename,Ancestral_allele,SIFT_pred,Polyphen2_HVAR_pred,CADD_phred,gnomAD_exomes_NFE_AF,gnomAD_exomes_POPMAX_AF,gnomAD_genomes_NFE_AF,gnomAD_genomes_POPMAX_AF,clinvar_hgvs,Interpro_domain,GTEx_V7_gene,GTEx_V7_tissue \
-a -m $SNPEFF_ANNOTATED_VCF  > $SNPSIFT_ANNOTATED_VCF

# Compress and Idx
bgzip $SNPSIFT_ANNOTATED_VCF
tabix "${SNPSIFT_ANNOTATED_VCF}.gz"