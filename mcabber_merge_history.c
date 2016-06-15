/*
 * mcabber_merge_history - merge mcabber history files
 * Copyright (C) 2016 Benjamin Abendroth
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <err.h>
#include <error.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <sys/types.h>

/*
 * Mcabber history entry
 */
struct hist_entry
{
   // Holds message type (MR, MS)
   char type[3];

   // Holds timestamp (20100901T13:39:14Z)
   char timestamp[19];

   // Holds count of following lines (000, 001, ...)
   char follow_lines[4];

   // Holds all lines belonging to this message.
   // NULL-terminated array, like argv.
   char **lines;
};

/*
 * Frees an hist_entry
 */
void free_hist_entry
 (
   struct hist_entry *entry
 )
{
   for (char **it = entry->lines; *it; ++it)
      free(*it);
   free(entry->lines);
   free(entry);
}

/*
 * Frees an array of entries
 */
void free_hist_entries
 (
   struct hist_entry **entries,
   int size
 )
{
   for (int i = 0; i < size; ++i)
      free_hist_entry(entries[i]);
   free(entries);
}


/*
 * Generic bubble sort algorithm
 */
void bubble_sort
 (
   void **array,
   size_t size,
   int (*compare)(const void*, const void*)
 )
{
   int n = size;

   do {
      int new_n = 1;

      for (int i = 0; i < n - 1; ++i) {
         if (compare(array[i], array[i+1]) > 0) {
            void *tmp = array[i];
            array[i] = array[i+1];
            array[i+1] = tmp;

            new_n = i + 1;
         }
      }
      n = new_n;
   }
   while (n > 1);
}

/*
 * Compare function for bubblesort
 */
int cmp_hist_entry_timestamp(const void* a, const void* b)
{
   return strcmp(
      ((struct hist_entry*) a)->timestamp,
      ((struct hist_entry*) b)->timestamp
   );
}

/*
 * Fully compare two hist entries
 */
//inline
int eq_hist_entry
 (
  const struct hist_entry *a,
  const struct hist_entry *b
 )
{
   if (
         strcmp(a->type, b->type) ||
         strcmp(a->timestamp, b->timestamp) ||
         strcmp(a->follow_lines, b->follow_lines)
      )
         return 0;

   for (char **a_it = a->lines, **b_it = b->lines; *a_it || *b_it; ++a_it, ++b_it)
      if (strcmp(*a_it, *b_it))
         return 0;

   return 1;
}

/*
 * Write out original mcabber history line
 */
//inline
void write_entry
 (
   struct hist_entry *entry,
   FILE *out_stream
 )
{
   fputs(entry->type, out_stream);
   fputc(' ', out_stream);
   fputs(entry->timestamp, out_stream);
   fputc(' ', out_stream);
   fputs(entry->follow_lines, out_stream);
   fputc(' ', out_stream);
   
   for (char **it = entry->lines; *it; ++it)
      fputs(*it, out_stream);
}


/*
 * Create a hist_entry struct by reading file stream.
 * Returns pointer to hist_entry or NULL if failed.
 */
struct hist_entry* read_entry
 (
   FILE *hist_fh
 )
{
   struct hist_entry *entry = calloc(1, sizeof(struct hist_entry));
   if (! entry) {
      perror("malloc");
      return NULL;
   }

   fgets(entry->type, sizeof(entry->type), hist_fh);
   if (strlen(entry->type) != 2) {
      free(entry);
      return NULL;
   }

   fgetc(hist_fh);
   fgets(entry->timestamp, sizeof(entry->timestamp), hist_fh);
   fgetc(hist_fh);
   fgets(entry->follow_lines, sizeof(entry->follow_lines), hist_fh);
   fgetc(hist_fh);

   int follow_lines = atoi(entry->follow_lines);
   entry->lines = calloc((2 + follow_lines), sizeof(char *));

   for (int i = 0; i <= follow_lines; ++i) {
      size_t line_size = 0;

      if (getline(&entry->lines[i], &line_size, hist_fh) == -1) {
         warn("Missing lines!");
         free_hist_entry(entry);
         return NULL;
      }
   }

   return entry;
}

/*
 * Insert hist_entry pointer.
 * Returns 1 on success or 0 on error.
 */
int insert_hist_entry
 (
   struct hist_entry ***entries,
   int *size,
   struct hist_entry *entry,
   int pre_alloc_size
 )
{
   struct hist_entry **new_entries;
   
   if (! *size) {
      new_entries = realloc(*entries, pre_alloc_size * sizeof(struct hist_entry *));
   }
   else if (*size % pre_alloc_size) {
      (*entries)[ (*size)++ ] = entry;
      return 1;
   }
   else {
      new_entries = realloc(*entries, ((*size)+pre_alloc_size) * sizeof(struct hist_entry *));
   }

   if (! new_entries) {
      perror("realloc");
      return 0;
   }

   new_entries[ (*size)++ ] = entry;
   *entries = new_entries;

   return 1;
}

/*
 * Create an array of hist_entry pointers out of file stream.
 */
struct hist_entry** read_hist
 (
   FILE *hist_fh,
   int *n_entries
 )
{
   *n_entries = 0;
   struct hist_entry **entries = NULL;
   struct hist_entry *entry;

   while (entry = read_entry(hist_fh)) {
      if (! insert_hist_entry(&entries, n_entries, entry, 1000)) {
         perror("realloc");
         free_hist_entries(entries, *n_entries);
         return NULL;
      }
   }

   bubble_sort((void **) entries, *n_entries, cmp_hist_entry_timestamp);
   return entries;
}

/*
 * Merge two list of entries and write the result to file stream.
 */
void merge_entries
 (
   struct hist_entry **entries_a,
   int n_entries_a,
   struct hist_entry **entries_b,
   int n_entries_b,
   FILE *out_stream
 )
{
   int i_a = 0;
   int i_b = 0;
   int ts_cmp;

   while (i_a < n_entries_a && i_b < n_entries_b) {
      ts_cmp = strcmp(entries_a[i_a]->timestamp, entries_b[i_b]->timestamp);

      if (ts_cmp <= 0) {
         // exactly same, skip b, write a
         if (ts_cmp == 0 && eq_hist_entry(entries_a[i_a], entries_b[i_b])) {
            ++i_b;
         }

         write_entry(entries_a[i_a++], out_stream);
      }
      else {
         write_entry(entries_b[i_b++], out_stream);
      }
   }

   while (i_a < n_entries_a)
      write_entry(entries_a[i_a++], out_stream);

   while (i_b < n_entries_b)
      write_entry(entries_b[i_b++], out_stream);
}

/*
 * Merge two files into one outfile
 * Returns 1 on success, 0 on failure.
 */
int merge_files
 (
   const char *file1,
   const char *file2,
   const char *fileO
 )
{
   FILE   *file_fh;
   struct hist_entry **hist1, **hist2;
   int    n_hist1, n_hist2;

   printf("Merging: %s + %s -> %s\n", file1, file2, fileO);

   if (! (file_fh = fopen(file1, "r"))) {
      perror(file1);
      return 0;
   }
   if (! (hist1 = read_hist(file_fh, &n_hist1))) {
      warn("%s: Error reading history file", file1);
      fclose(file_fh);
      return 0;
   }
   
   if (! (file_fh = freopen(file2, "r", file_fh))) {
      perror(file2);
      free_hist_entries(hist1, n_hist1);
      return 0;
   }
   if (! (hist2 = read_hist(file_fh, &n_hist2))) {
      warn("%s: errors reading history file", file2);
      free_hist_entries(hist1, n_hist1);
      fclose(file_fh);
      return 0;
   }

   if (! (file_fh = freopen(fileO, "w", file_fh))) {
      perror(fileO);
      free_hist_entries(hist1, n_hist1);
      free_hist_entries(hist2, n_hist2);
      return 0;
   }

   merge_entries(hist1, n_hist1, hist2, n_hist2, file_fh);
   free_hist_entries(hist1, n_hist1);
   free_hist_entries(hist2, n_hist2);
   fclose(file_fh);
   return 1;
}

/*
 * Copies source to dest. If source and dest are the same file nothing
 * is done and 1 is returned.
 * Returns 1 on success, 0 on failure
 */
int copy
 (
   const char *source,
   const char *dest
 )
{
   FILE *source_fh, *dest_fh = NULL;

   struct stat statbuf;
   if (stat(source, &statbuf) == -1) {
      perror(source);
      return 0;
   }

   int source_ino = statbuf.st_ino;
   int size = statbuf.st_size;

   // file exists, check if is same file
   if (stat(dest, &statbuf) != -1) {
      if (source_ino == statbuf.st_ino) {
         return 1;
      }
   }

   if (! (source_fh = fopen(source, "r"))) {
      perror(source);
      return 0;
   }

   if (! (dest_fh = fopen(dest, "w"))) {
      fclose(source_fh);
      perror(dest);
      return 0;
   }

   if (sendfile(fileno(dest_fh), fileno(source_fh), NULL, size) == -1) {
      fclose(source_fh);
      fclose(dest_fh);
      perror("copy");
      return 0;
   }

   fclose(source_fh);
   fclose(dest_fh);
   return 1;
}

int merge_dirs
 (
   const char *dir1,
   const char *dir2,
   const char *dirO
 )
{
   int status = 1;
   DIR *dir_fh;
   struct dirent *file;

   dir_fh = opendir(dir1);
   if (! dir_fh) {
      perror(dir1);
      return 0;
   }

   while (file = readdir(dir_fh)) {

      if (file->d_type == DT_DIR)
         continue;
      if (! strcmp(file->d_name, ".") || ! strcmp(file->d_name, ".."))
         continue;

      char *file1_path = malloc( strlen(dir1) + 2 + strlen(file->d_name) );
      strcpy(file1_path, dir1);
      strcat(file1_path, "/");
      strcat(file1_path, file->d_name);

      char *file2_path = malloc( strlen(dir2) + 2 + strlen(file->d_name) );
      strcpy(file2_path, dir2);
      strcat(file2_path, "/");
      strcat(file2_path, file->d_name);

      char *fileO_path = malloc( strlen(dirO) + 2 + strlen(file->d_name) );
      strcpy(fileO_path, dirO);
      strcat(fileO_path, "/");
      strcat(fileO_path, file->d_name);

      if (access(file2_path, F_OK) != -1) {
         status &= merge_files(file1_path, file2_path, fileO_path);
      }
      else {
         status &= copy(file1_path, fileO_path);
      }

      free(file1_path);
      free(file2_path);
      free(fileO_path);
   }
   closedir(dir_fh);


   dir_fh = opendir(dir1);
   if (! dir_fh) {
      perror(dir1);
      return 0;
   }

   while (file = readdir(dir_fh)) {

      if (file->d_type == DT_DIR)
         continue;
      if (! strcmp(file->d_name, ".") || ! strcmp(file->d_name, ".."))
         continue;

      char *file1_path = malloc( strlen(dir1) + 2 + strlen(file->d_name) );
      strcpy(file1_path, dir1);
      strcat(file1_path, "/");
      strcat(file1_path, file->d_name);

      char *file2_path = malloc( strlen(dir2) + 2 + strlen(file->d_name) );
      strcpy(file2_path, dir2);
      strcat(file2_path, "/");
      strcat(file2_path, file->d_name);

      char *fileO_path = malloc( strlen(dirO) + 2 + strlen(file->d_name) );
      strcpy(fileO_path, dirO);
      strcat(fileO_path, "/");
      strcat(fileO_path, file->d_name);

      if (access(file1_path, F_OK) == -1) {
         status &= copy(file2_path, fileO_path);
      }

      free(file1_path);
      free(file2_path);
      free(fileO_path);
   }
   closedir(dir_fh);

   return status;
}

void help(const char *prg)
{
   fprintf(stderr,
    "Merge mcabber history dirs\n\n"
    "Usage:\n"
    "\t%s dir1 dir2 [outdir]\n"
    "\t%s file1 file2 [outfile]\n\n"
    "If outdir our outfile are missing this program works inplace.\n"
   ,prg,prg);
      
   exit(1);
}

int main(int argc, char **argv)
{
   struct stat statbuf;
   int source1_is_dir = 0;

   if (argc < 3 || argc > 4)
      help(argv[0]);

   for (int i = 1; i < argc; ++i)
      if (strcmp(argv[i], "--"))
         break;
      else if (strcmp(argv[i], "--help") || strcmp(argv[i], "-h"))
         help(argv[0]);

   // check first arg, determine type
   if (stat(argv[1], &statbuf) == -1) {
      perror(argv[1]);
      return 1;
   }
   source1_is_dir = S_ISDIR(statbuf.st_mode);

   // check second arg, check if type matches first arg
   if (stat(argv[2], &statbuf) == -1) {
      perror(argv[2]);
      return 1;
   }
   if (source1_is_dir != S_ISDIR(statbuf.st_mode))
      errx(1, "Both must be dir or file");

   // we got third arg
   if (argc == 4) {
      // first two args were directories, the third one must be one, too
      if (source1_is_dir) {
         if (stat(argv[3], &statbuf) == -1 ) {
            perror(argv[3]);
            return 1;
         }
         if (! S_ISDIR(statbuf.st_mode)) {
            errx(1, "Dest has to be directory");
         }

         return ! merge_dirs(argv[1], argv[2], argv[3]);
      }
      else {
         return ! merge_files(argv[1], argv[2], argv[3]);
      }
   }
   else if (source1_is_dir) {
      return ! merge_dirs(argv[1], argv[2], argv[1]);
   }
   else {
      return ! merge_files(argv[1], argv[2], argv[1]);
   }
}
