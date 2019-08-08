#!/usr/bin/env python3

"""
Classes for running one or several mpi-runs with BOUT++ at once.
Read the docstring of "basic_runner", or refer to the user manual of
BOUT++ for more info. Examples can be found in
BOUT/examples/bout_runners_example.
"""

# NOTE: This document uses folding. A hash-symbol followed by three {'s
#       denotes the start of a fold, and a hash-symbol followed by three
#       }'s denotes the end of a fold
# NOTE: Improvement suggestions:
#       It would be beneficial to refactor bout_runners
#       1. Better design: Shorter functions
#       2. Better input parsing: The input for the constructors are rather long.
#          One alternative could be to have setters for a grouping of
#          parameters
from old_code.overview import _warning_printer, _run_make, _create_folder, \
    _check_if_set_correctly, _check_for_correct_type, \
    _error_check_additional, \
    _error_check_for_run_input

__authors__ = "Michael Loeiten"
__version__ = "1.08"
__date__ = "2018.01.07"

import os
import re
import itertools
import glob
import datetime
import time
import shutil
from numbers import Number
import numpy as np
from boututils.run_wrapper import shell, getmpirun
from boututils.options import BOUTOptions
from boututils.datafile import DataFile
from boutdata.restart import redistribute, addnoise, resizeZ, resize

# As a child class uses the super function, the class must allow an
# object as input


class basic_runner(object):
    """
    basic_runner
    ------------

    Class for mpi running one or several runs with BOUT++.
    Calling self.execute_runs() will run your BOUT++ program with the possible
    combinations given in the member data using the mpi runner.

    Before each run basic_runner will:
        * Create a folder system, based on the member data, rooted in
          self._directory.
        * Copy BOUT.inp of self._directory to the execution folder.
        * Check that the domain split is sane (suggest a split if a bad
          domain split is given)

    If the restart option is checked, bout_runners will
        * Put old data into a restart folder (so that nothing is lost
          upon restart)
        * Resize the mesh if new sizes are detected

    A log-file for the run is stored in self._directory

    By default self._directory = "data", self._nproc = 1 and
    self._allow_size_modification = False

    self._program_name is by default set to the same name as any .o files in
    thefolder where an instance of the object is created. If none is found the
    constructor tries to run make.

    All other data members are set to None by default.

    The data members will override the corresponding options given in
    self._directory/BOUT.inp.

    See the doctring of the constructor (__int__) for options.
    See BOUT/examples/bout_runners_example for examples.
    """

    def _create_run_log(self):
        """Makes a run_log file if it doesn't exists"""

        # Checks if run_log exists
        self._run_log = os.path.join(self._directory, "run_log.txt")
        if os.path.isfile(self._run_log) == False:
            # The header
            header = ("start_time", "run_type", "run_no",
                      "run_time_H:M:S", "dump_folder")
            header_format = "{:<19}   {:<9}   {:<6}   {:<17}   {:<}"
            # Create the log file, and print the header
            with open(self._run_log, "w") as f:
                f.write(header_format.format(*header) + "\n")

        # Preparation of the run
        print("\nRunning with inputs from '{}'".format(self._directory))

    def _get_correct_domain_split(self):
        """
        Checks that the grid can be split in the correct number of
        processors.

        If not, vary the number of points until value is found.
        """

        if (self._nx is None) and (self._ny is None):
            # Set the local nx value
            local_nx = [self._get_dim_from_input("nx")]

            # Set the local ny value
            local_ny = [self._get_dim_from_input("ny")]
        elif (self._nx is None):
            # ny is given, so we only need to find nx
            local_ny = list(self._ny)

            # Set the local nx value
            local_nx = [self._get_dim_from_input("nx")]

            # Get the same length on nx and ny
            local_nx = local_nx * len(local_ny)
        elif (self._ny is None):
            # nx is given, so we only need to find ny
            local_nx = list(self._nx)

            # Set the local ny value
            local_ny = [self._get_dim_from_input("ny")]

            # Get the same length on nx and ny
            local_ny = local_ny * len(local_nx)
        else:
            local_nx = list(self._nx)
            local_ny = list(self._ny)

        # If NXPE is not set, we will try to find a optimal grid size
        # Flag to determine if a warning should be printed
        produce_warning = False
        print("\nChecking the grid split for the meshes\n")
        # Obtain MXG
        MXG, _MYG = self._get_MXG_MYG()
        if self._NXPE is None:
            for size_nr in range(len(local_nx)):
                print("Checking nx={}  and ny={}".
                      format(local_nx[size_nr], local_ny[size_nr]))
                # Check to see if succeeded
                init_split_found = False
                cur_split_found = False
                add_number = 1
                # Counter to see how many times the while loop has been
                # called
                count = 0

                while cur_split_found == False:
                    # The same check as below is performed internally in
                    # BOUT++ (see boutmesh.cxx under
                    # if(options->isSet("NXPE")))
                    for i in range(1, self._nproc + 1, 1):
                        MX = local_nx[size_nr] - 2 * MXG
                        # self._nproc is called NPES in boutmesh
                        if (self._nproc % i == 0) and \
                           (MX % i == 0) and \
                           (local_ny[size_nr] % (self._nproc / i) == 0):
                            # If the test passes
                            cur_split_found = True

                    # Check if cur_split_found is true, eventually
                    # update the add_number
                    local_nx, local_ny, add_number, produce_warning\
                        = self._check_cur_split_found(cur_split_found,
                                                      produce_warning,
                                                      add_number,
                                                      size_nr,
                                                      local_nx,
                                                      local_ny,
                                                      using_nx=True,
                                                      using_ny=True)

                    # This will be used if self_allow_size_modification is
                    # off, or if we are using a grid file
                    if count == 0 and cur_split_found:
                        init_split_found = True

                    # Add one to the counter
                    count += 1

                # Check if initial split succeeded
                self._check_init_split_found(init_split_found,
                                             size_nr,
                                             local_nx,
                                             local_ny,
                                             test_nx=True,
                                             test_ny=True,
                                             produce_warning=produce_warning)
        else:
            # Check if NXPE and NYPE is set consistently with nproc
            self._check_NXPE_or_NYPE(local_nx,
                                     local_ny,
                                     type_str="NXPE",
                                     MXG=MXG)
            self._check_NXPE_or_NYPE(local_nx,
                                     local_ny,
                                     type_str="NYPE")

    def _call_post_processing_function(
            self,
            function=None,
            folders=None,
            **kwargs):
        """Function which calls the post_processing_function"""

        function(folders, **kwargs)

    def _check_if_same_len(self, object1=None, object2=None):
        """Checks if object1 and object2 has the same length

        Input:
        object1 - a tuple of the object [0] and its name [1]
        object2 - a tuple an object [0] different than object1 together with
                  its name [1]
        """

        try:
            len_dim1 = len(object1[0])
        # If object1 does not have length
        except TypeError:
            len_dim1 = 1
        try:
            len_dim2 = len(object2[0])
        # If object2 does not have length
        except TypeError:
            len_dim2 = 1

        if len_dim1 != len_dim2:
            message = ("{} and {} must have the same"
                       " length when specified").format(object1[1], object2[1])
            self._errors.append("RuntimeError")
            raise RuntimeError(message)

    def _check_cur_split_found(self,
                               cur_split_found,
                               produce_warning,
                               add_number,
                               size_nr,
                               local_nx,
                               local_ny,
                               using_nx=None,
                               using_ny=None):
        """
        Checks if the current split is found.

        Will add a number if not found.

        Parameters
        ----------
        cur_split_found : bool
            Whether or not the current split was found
        produce_warning : bool
            If a warning should be produced
        add_number : int
            The number added to nx and/or ny
        local_nx : [int|sequence of int]
            Sequence of values of nx (a local value is used in order not to
            alter self._nx)
        local_ny : [int|sequence of int]
            Sequence of values of ny (a local value is used in order not to
            alter self._ny)
        size_nr : int
            Index of the current nx and/or ny
        using_nx : bool
            If add_number should be added to nx
        using_ny : bool
            if add_number should be added to ny

        Returns
        -------
        local_nx : [int|sequence of int]
            Sequence of values of nx
        local_ny : [int|sequence of int]
            Sequence of values of ny
        add_number : int
            The number to eventually be added the next time
        produce_warning : bool
            Whether or not a warning should be produced
        """

        # If the value tried is not a good value
        if not cur_split_found:
            # Produce a warning
            produce_warning = True
            if using_nx:
                local_nx[size_nr] += add_number
            if using_ny:
                local_ny[size_nr] += add_number

            print("Mismatch, trying {}*{}".
                  format(local_nx[size_nr], local_ny[size_nr]))

            # FIXME: This is a crude approach as we are adding one to
            #        both nx and ny
            #        Consider: Something like this
            #        nx+1   ny
            #        nx     ny+1
            #        nx-1   ny
            #        nx     ny-1
            #        nx+2   ny
            #        nx     ny+2
            #        nx-2   ny
            #        nx     ny-2
            #        ...
            add_number = (-1)**(abs(add_number))\
                * (abs(add_number) + 1)
        else:
            # If no warnings has been produced so far
            if not(produce_warning):
                produce_warning = False

        return local_nx, local_ny, add_number, produce_warning

    def _check_init_split_found(self,
                                init_split_found,
                                size_nr,
                                local_nx,
                                local_ny,
                                test_nx=None,
                                test_ny=None,
                                produce_warning=None):
        """
        Check if the initial split was a good choice when checking the grids.

        Will raise eventual errors.

        Parameters
        ----------
        init_split_found : bool
            Whether or not a good split was found on the first trial
        size_nr : int
            The index of the current nx, ny or NXPE under consideration
        local_nx : [int|sequence of int]
            Sequence of values of nx (a local value is used in order not to
            alter self._nx)
        local_ny : [int|sequence of int]
            Sequence of values of ny (a local value is used in order not to
            alter self._ny)
        test_nx : bool
            whether or not the test was run on nx
        test_ny : bool
            whether or not the test was run on ny
        produce_warning : bool
            whether or not a warning should be produced
        """

        if not(init_split_found):
            # If modification is allowed
            if not(self._allow_size_modification) or\
                  (self._grid_file is not None):
                # If the split fails and the a grid file is given
                if self._grid_file is not None:
                    self._errors.append("RuntimeError")
                    message = ("The grid can not be split using the"
                               " current number of nproc.\n"
                               "Suggest using ")
                    if test_nx:
                        message += "nx = {} ".format(self._nx[size_nr])
                    if test_ny:
                        message += "ny = {} ".format(self._ny[size_nr])
                    message += " with the current nproc"
                    raise RuntimeError(message)
                # If the split fails and no grid file is given
                else:
                    self._errors.append("RuntimeError")
                    message = ("The grid can not be split using the"
                               " current number of nproc.\n"
                               "Setting allow_size_modification = True"
                               " will allow modification of the grid"
                               " so that it can be split with the"
                               " current number of nproc")
                    raise RuntimeError(message)
            else:
                # Set nx and ny
                self._nx = local_nx
                self._ny = local_ny

        print("Successfully found the following good values for the mesh:")
        message = ""
        if test_nx:
            message += "nx = {} ".format(local_nx[size_nr])
        if test_ny:
            message += "ny = {} ".format(local_ny[size_nr])

        print(message + "\n")

        if produce_warning:
            message = "The mesh was changed to allow the split given by nproc"
            _warning_printer(message)
            self._warnings.append(message)

    def _check_NXPE_or_NYPE(self,
                            local_nx,
                            local_ny,
                            type_str=None,
                            MXG=None,
                            produce_warning=None,
                            ):
        """
        Check if NXPE or NYPE is consistent with nproc

        Parameters
        ----------

        local_nx : [int|sequence of int]
            Sequence of values of nx (a local value is used in order not to
            alter self._nx)
        local_ny : [int|sequence of int]
            Sequence of values of ny (a local value is used in order not to
            alter self._ny)
        type_str : ["NXPE" | "NYPE"]
            Can be either "NXPE" or "NYPE" and is specifying whether
            NXPE or NYPE should be checked
        MXG : int
            The current MXG
        produce_warning : bool
            Whether or not a warning should be produced
        """

        for size_nr in range(len(local_nx)):
            # Check the type
            if type_str == "NXPE":
                print("Checking nx = {} with NXPE = {}".
                      format(local_nx[size_nr], self._NXPE[size_nr]))
            elif type_str == "NYPE":
                print("Checking ny = {} with NYPE = {}".
                      format(local_ny[size_nr], self._NYPE[size_nr]))
            # Check to see if succeeded
            init_split_found = False
            cur_split_found = False
            add_number = 1
            # Counter to see how many times the while loop has been
            # called
            count = 0

            while cur_split_found == False:
                # The same check as below is performed internally in
                # BOUT++ (see boutmesh.cxx under
                # if((MX % NXPE) != 0)
                # and
                # if((MY % NYPE) != 0)
                if type_str == "NXPE":
                    MX = local_nx[size_nr] - 2 * MXG
                    # self._nproc is called NPES in boutmesh
                    if (MX % self._NXPE[size_nr]) == 0:
                        # If the test passes
                        cur_split_found = True
                    # Check if cur_split_found is true, eventually
                    # update the add_number
                    local_nx, local_ny, add_number, produce_warning\
                        = self._check_cur_split_found(cur_split_found,
                                                      produce_warning,
                                                      add_number,
                                                      size_nr,
                                                      local_nx,
                                                      local_ny,
                                                      using_nx=True,
                                                      using_ny=False)
                elif type_str == "NYPE":
                    MY = local_ny[size_nr]
                    # self._nproc is called NPES in boutmesh
                    if (MY % self._NYPE[size_nr]) == 0:
                        # If the test passes
                        cur_split_found = True
                    # Check if cur_split_found is true, eventually
                    # update the add_number
                    local_nx, local_ny, add_number, produce_warning\
                        = self._check_cur_split_found(cur_split_found,
                                                      produce_warning,
                                                      add_number,
                                                      size_nr,
                                                      local_nx,
                                                      local_ny,
                                                      using_nx=False,
                                                      using_ny=True)

                # This will be used if self_allow_size_modification is
                # off, or if we are using a grid file
                if count == 0 and cur_split_found:
                    init_split_found = True

                # Add one to the counter
                count += 1

            # Check if initial split succeeded
            if type_str == "NXPE":
                self._check_init_split_found(init_split_found,
                                             size_nr,
                                             local_nx,
                                             local_ny,
                                             test_nx=True,
                                             test_ny=False,
                                             produce_warning=produce_warning)
            elif type_str == "NYPE":
                self._check_init_split_found(init_split_found,
                                             size_nr,
                                             local_nx,
                                             local_ny,
                                             test_nx=False,
                                             test_ny=True,
                                             produce_warning=produce_warning)

    def _get_folder_name(self, combination):
        """
        Returning the folder name where the data will be stored.

        If all options are given the folder structure should be on the
        form solver/method/nout_timestep/mesh/additional/grid
        """

        # Combination is one of the combination of the data members
        # which is used as the command line arguments in the run
        combination = combination.split()

        # FIXME: The grid-file names can become long if adding these,
        #        consider using just path name to gridfile
        # If there is a grid file, we will extract the values from the
        # file, and put it into this local combination variable, so that
        # a proper dmp folder can be made on basis on the variables
        # A flag to see whether or not the grid file was found
        grid_file_found = False
        # Check if grid is in element, and extract its path
        for elem in combination:
            if elem[0:5] == "grid=":
                cur_grid = elem.replace("grid=", "")
                grid_file_found = True

        # If the grid file is found, open it
        if grid_file_found:
            # Open (and automatically close) the grid files
            f = DataFile(cur_grid)
            # Search for mesh types in the grid file
            mesh_types = (
                ("mesh:", "nx"),
                ("mesh:", "ny"),
                ("mesh:", "nz"),
                ("mesh:", "zperiod"),
                ("mesh:", "zmin"),
                ("mesh:", "zmax"),
                ("mesh:", "dx"),
                ("mesh:", "dy"),
                ("mesh:", "dz"),
                ("mesh:", "ixseps1"),
                ("mesh:", "ixseps2"),
                ("mesh:", "jyseps1_1"),
                ("mesh:", "jyseps1_2"),
                ("mesh:", "jyseps2_1"),
                ("mesh:", "jyseps2_2"),
                ("", "MXG"),
                ("", "MYG"),
            )
            for mesh_type in mesh_types:
                grid_variable = f.read(mesh_type[1])
                # If the variable is found
                if grid_variable is not None:
                    if len(grid_variable.shape) > 0:
                        # Chosing the first
                        grid_variable =\
                            "{:.2e}".format(grid_variable.flatten()[0])
                    # Append it to the combinations list
                    combination.append("{}{}={}".format(mesh_type[0],
                                                        mesh_type[1],
                                                        grid_variable))

        # Make lists for the folder-type, so that we can append the
        # elements in the combination folders if it is found
        solver = []
        method = []
        nout_timestep = []
        mesh = []
        additional = []
        grid_file = []

        # We will loop over the names describing the methods used
        # Possible directional derivatives
        dir_derivatives = ("ddx", "ddy", "ddz")

        # Check trough all the elements of combination
        for elem in combination:

            # If "solver" is in the element
            if "solver" in elem:
                # Remove 'solver:' and append it to the mesh folder
                cur_solver = elem.replace("solver:", "")
                cur_solver = cur_solver.replace("=", "_")
                # Append it to the solver folder
                solver.append(cur_solver)

            # If nout or timestep is in the element
            elif ("nout" in elem) or\
                 ("timestep" in elem):
                # Remove "=", and append it to the
                # nout_timestep folder
                nout_timestep.append(elem.replace("=", "_"))

            # If any quantity related to mesh is in the combination
            elif ("mesh" in elem) or\
                 ("MXG" in elem) or\
                 ("MYG" in elem) or\
                 ("NXPE" in elem) or\
                 ("NYPE" in elem) or\
                 ("zperiod" in elem) or\
                 ("zmin" in elem) or\
                 ("zmax" in elem) or\
                 (("dx" in elem) and not("ddx" in elem)) or\
                 (("dy" in elem) and not("ddy" in elem)) or\
                 (("dz" in elem) and not("ddz" in elem)):
                # Remove "mesh:", and append it to the mesh folder
                cur_mesh = elem.replace("mesh:", "")
                cur_mesh = cur_mesh.replace("=", "_")
                # Simplify the mesh spacing
                if ("dx" in elem) or ("dy" in elem) or ("dz" in elem):
                    cur_mesh = cur_mesh.split("_")
                    cur_mesh = "{}_{:.2e}".format(
                        cur_mesh[0], float(cur_mesh[1]))
                mesh.append(cur_mesh)

            # If a grid file is in the combination
            elif (elem[0:4] == "grid"):
                # Remove .grd .nc and =
                cur_grid = elem.replace(".grd", "")
                cur_grid = cur_grid.replace(".nc", "")
                cur_grid = cur_grid.replace("=", "_")
                grid_file.append(cur_grid)

            # If the element is none of the above
            else:
                # It could either be a dir derivative
                # Set a flag to state if any of the dir derivative was
                # found in the combination
                dir_derivative_set = False
                # If any of the methods are in combination
                for dir_derivative in dir_derivatives:
                    if dir_derivative in elem:
                        # Remove ":", and append it to the
                        # method folder
                        cur_method = elem.replace(":", "_")
                        cur_method = cur_method.replace("=", "_")
                        method.append(cur_method)
                        dir_derivative_set = True

                # If the dir_derivative_set was not set, the only
                # possibility left is that the element is an
                # "additional" option
                if not(dir_derivative_set):
                    # Replace ":" and "=" and append it to the
                    # additional folder
                    cur_additional = elem.replace(":", "_")
                    cur_additional = cur_additional.replace("=", "_")
                    cur_additional = cur_additional.replace('"', "-")
                    cur_additional = cur_additional.replace("'", "-")
                    cur_additional = cur_additional.replace("(", ",")
                    cur_additional = cur_additional.replace(")", ",")
                    additional.append(cur_additional)

        # We sort the elements in the various folders alphabetically,
        # to ensure that the naming convention is always the same, no
        # matter how the full combination string looks like
        # Sort alphabetically
        solver.sort()
        # We want "type" to be first, and "atol" and "rtol" to be last
        sort_these = (
            ("type", 0),
            ("atol", -1),
            ("rtol", -1)
        )
        # Loop through everything we want to sort
        for sort_this in sort_these:
            # Flag to check if found
            found_string = False
            for elem_nr, elem in enumerate(solver):
                if sort_this[0] in elem:
                    swap_nr = elem_nr
                    # Set the flag that the string is found
                    found_string = True
            # If type was found
            if found_string:
                # Swap the elements in the solver
                solver[sort_this[1]], solver[swap_nr] =\
                    solver[swap_nr], solver[sort_this[1]]
        method.sort()
        nout_timestep.sort()
        mesh.sort()
        additional.sort()
        grid_file.sort()

        # Combine the elements in the various folders
        solver = ("_".join(solver),)
        method = ("_".join(method),)
        nout_timestep = ("_".join(nout_timestep),)
        mesh = ("_".join(mesh),)
        additional = ("_".join(additional),)
        grid_file = ("_".join(grid_file),)

        # Put all the folders into the combination_folder
        combination_folder = (
            solver,
            method,
            nout_timestep,
            mesh,
            additional,
            grid_file
        )
        # We access the zeroth element (if given) as the folders are
        # given as a sequence
        combination_folder = tuple(folder[0] for folder in combination_folder
                                   if (len(folder) != 0) and not("" in folder))

        # Make the combination folder as a string
        combination_folder = "/".join(combination_folder)

        return combination_folder

    def _set_member_data(self, input_parameter):
        """
        Returns the input_parameter as a tuple if it is different than None,
        and if it is not iterable
        """

       # If the input_data is not set, the value in BOUT.inp will
       # be used
        if input_parameter is not None:
            # If the input_data is not an iterable, or if it is a
            # string: Put it to a tuple
            if not(hasattr(input_parameter, "__iter__")) or\
               (type(input_parameter)) == str:
                input_parameter = (input_parameter,)

        return input_parameter

    def _copy_run_files(self):
        """
        Function which copies run files from self._cur_restart_from
        """

        do_run =\
            self._check_if_run_already_performed(
                restart_file_search_reason="restart_from")

        if do_run:
            print("\nCopying files from {0} to {1}\n".
                  format(self._cur_restart_from, self._dmp_folder))

            # Files with these extension will be given the
            # additional extension .cpy when copied to the destination
            # folder
            extensions_w_cpy = ["inp"]
            # When the extension is not a real extension
            has_extensions_w_cpy = ["log.*"]

            if self._cpy_source:
                extensions_w_cpy.extend(["cc", "cpp", "cxx", "C", "c++",
                                         "h", "hpp", "hxx", "h++"])

            # Python 3 syntax (not python 2 friendly)
            # extensions =\
            #    (*extensions_w_cpy, *has_extensions_w_cpy, "restart.*")
            extensions = extensions_w_cpy
            for item in has_extensions_w_cpy:
                extensions.append(item)
            extensions.append("restart.*")

            if self._restart == "append":
                extensions.append("dmp.*")

            # Copy for all files in the extension
            for extension in extensions:
                file_names = glob.glob(
                    os.path.join(
                        self._cur_restart_from,
                        "*." + extension))
                for cur_file in file_names:
                    # Check if any of the extensions matches the current
                    # string
                    if any([cur_file.endswith(ewc)
                            for ewc in extensions_w_cpy]):
                        # Add ".cpy" to the file name (without the path)
                        name = os.path.split(cur_file)[-1] + ".cpy"
                        shutil.copy2(cur_file,
                                     os.path.join(self._dmp_folder, name))
                    # When the extension is not a real extension we must
                    # remove "*" in the string as shutil doesn't accept
                    # wildcards
                    elif any([hewc.replace("*", "") in cur_file
                              for hewc in has_extensions_w_cpy]):
                        # Add ".cpy" to the file name (without the path)
                        name = os.path.split(cur_file)[-1] + ".cpy"
                        shutil.copy2(cur_file,
                                     os.path.join(self._dmp_folder, name))
                    else:
                        shutil.copy2(cur_file, self._dmp_folder)

        return do_run

    def _move_old_runs(self, folder_name="restart", include_restart=False):
        """Move old runs, return the destination path"""

        # Check for folders in the dmp directory
        directories = tuple(
            name for name in
            os.listdir(self._dmp_folder) if
            os.path.isdir(os.path.join(
                self._dmp_folder, name))
        )
        # Find occurrences of "folder_name", split, and cast result to number
        restart_nr = tuple(int(name.split("_")[-1]) for name in directories
                           if folder_name in name)
        # Check that the sequence is not empty
        if len(restart_nr) != 0:
            # Sort the folders in ascending order
            restart_nr = sorted(restart_nr)
            # Pick the last index
            restart_nr = restart_nr[-1]
            # Add one to the restart_nr, as we want to create
            # a new directory
            restart_nr += 1
        else:
            # Set the restart_nr
            restart_nr = 0
        # Create the folder for the previous runs
        _create_folder(os.path.join(
            self._dmp_folder,
            "{}_{}".format(folder_name, restart_nr)))

        extensions_to_move = ["cpy", "log.*", "dmp.*",
                              "cc", "cpp", "cxx", "C", "c++",
                              "h", "hpp", "hxx", "h++"]

        if include_restart:
            extensions_to_move.append("restart.*")

        dst = os.path.join(self._dmp_folder,
                           "{}_{}".format(folder_name, restart_nr))

        print("Moving old runs to {}\n".format(dst))

        for extension in extensions_to_move:
            file_names =\
                glob.glob(os.path.join(self._dmp_folder, "*." + extension))

            # Cast to unique file_names
            file_names = set(file_names)

            # Move the files
            for cur_file in file_names:
                shutil.move(cur_file, dst)

        if not(include_restart):
            # We would like to save the restart files as well
            print("Copying restart files to {}\n".format(dst))
            file_names =\
                glob.glob(os.path.join(self._dmp_folder, "*.restart.*"))

            # Cast to unique file_names
            file_names = set(file_names)

            # Copy the files
            for cur_file in file_names:
                shutil.copy2(cur_file, dst)

        return dst

    def _append_run_log(self, start, run_no, run_time):
        """Appends the run_log"""

        # Convert seconds to H:M:S
        run_time = str(datetime.timedelta(seconds=run_time))

        start_time = "{}-{}-{}-{}:{}:{}".\
                     format(start.year, start.month, start.day,
                            start.hour, start.minute, start.second)

        # If the run is restarted with initial values from the last run
        if self._restart:
            dmp_line = "{}-restart-{}".format(self._dmp_folder, self._restart)
            if self._cur_restart_from:
                dmp_line += " from " + self._cur_restart_from
        else:
            dmp_line = self._dmp_folder

        # Line to write
        line = (start_time, self._run_type, run_no, run_time, dmp_line)
        # Opens for appending
        log_format = "{:<19}   {:^9}   {:^6}   {:<17}   {:<}"
        with open(self._run_log, "a") as f:
            f.write(log_format.format(*line) + "\n")

    def _get_swapped_input_list(self, input_list):
        """
        Finds the element in the input list, which corresponds to the
        self._sort_by criterion. The element is swapped with the last
        index, so that itertools.product will make this the fastest
        varying variable
        """

        # We make a sort list containing the string to find in the
        # input_list
        sort_list = []

        # We loop over the elements in self._sort_by to find what
        # string we need to be looking for in the elements of the lists
        # in input_list
        for sort_by in self._sort_by:
            # Find what list in the input_list which contains what we
            # would sort by

            if sort_by == "spatial_domain":
                # nx, ny and nz are all under the section "mesh"
                find_in_list = "mesh"

            elif sort_by == "temporal_domain":
                # If we are sorting by the temporal domain, we can either
                # search for timestep or nout
                if self._timestep is not None:
                    find_in_list = "timestep"
                elif self._nout is not None:
                    find_in_list = "nout"

            elif (sort_by == "ddx_first") or\
                 (sort_by == "ddx_second") or\
                 (sort_by == "ddx_upwind") or\
                 (sort_by == "ddx_flux") or\
                 (sort_by == "ddy_first") or\
                 (sort_by == "ddy_second") or\
                 (sort_by == "ddy_upwind") or\
                 (sort_by == "ddy_flux") or\
                 (sort_by == "ddz_first") or\
                 (sort_by == "ddz_second") or\
                 (sort_by == "ddz_upwind") or\
                 (sort_by == "ddz_flux"):
                find_in_list = sort_by.replace("_", ":")

            elif sort_by == "solver":
                find_in_list = sort_by

            else:
                find_in_list = sort_by

            # Append what to be found in the input_list
            sort_list.append(find_in_list)

        # For all the sort_list, we would like check if the match
        # can be found in any of the elements in input_list
        # Appendable list
        lengths = []
        for sort_nr, sort_by_txt in enumerate(sort_list):
            # Make a flag to break the outermost loop if find_in_list is
            # found
            break_outer = False
            # Loop over the lists in the input_list to find the match
            for elem_nr, elem in enumerate(input_list):
                # Each of the elements in this list is a string
                for string in elem:
                    # Check if fins_in_list is in the string
                    if sort_by_txt in string:
                        # If there is a match, store the element number
                        swap_from_index = elem_nr
                        # Check the length of the element (as this is
                        # the number of times the run is repeated, only
                        # changing the values of sort_by [defining a
                        # group])
                        lengths.append(len(elem))
                        # Break the loop to save time
                        break_outer = True
                        break
                # Break the outer loop if find_in_list_is_found
                if break_outer:
                    break

            # As it is the last index which changes the fastest, we swap the
            # element where the find_in_list was found with the last element
            input_list[swap_from_index], input_list[-(sort_nr + 1)] =\
                input_list[-(sort_nr + 1)], input_list[swap_from_index]

        # The number of runs in one "group"
        # Initialize self._len_group with one as we are going to
        # multiply it with all the elements in lengths
        self._len_group = 1
        for elem in lengths:
            self._len_group *= elem

        return input_list

    def _get_MXG_MYG(self):
        """Function which returns the MXG and MYG"""

        if self._MXG is None:
            try:
                MXG = eval(self._inputFileOpts.root["mxg"])
            except KeyError:
                message = ("Could not find 'MXG' or 'mxg' "
                           "in the input file. "
                           "Setting MXG = 2")
                _warning_printer(message)
                self._warnings.append(message)
                MXG = 2
        else:
            MXG = self._MXG
        if self._MYG is None:
            try:
                MYG = eval(self._inputFileOpts.root["myg"])
            except KeyError:
                message = ("Could not find 'MYG' or 'myg' "
                           "in the input file. "
                           "Setting MYG = 2")
                _warning_printer(message)
                self._warnings.append(message)
                MYG = 2
        else:
            MYG = self._MYG

        return MXG, MYG

    def _get_dim_from_input(self, direction):
        """
        Get the dimension from the input

        Parameters
        ----------
        direction : ["nx"|"ny"|"nz"|"mz"]
            The direction to read

        Returns
        -------
        Number of points in the given direction
        """

        # If nx and ny is a function of MXG and MYG
        MXG, MYG = self._get_MXG_MYG()
        # NOTE: MXG may seem unused, but it needs to be in the current
        #       namespace if eval(self._inputFileOpts.mesh["nx"]) depends on
        #       MXG

        if self._grid_file:
            # Open the grid file and read it
            with DataFile(self._grid_file) as f:
                # Loop over the variables in the file
                n_points = f.read(direction)
        else:
            try:
                n_points = eval(self._inputFileOpts.mesh[direction])
            except NameError:
                message = "Could not evaluate\n"
                message += self._inputFileOpts.mesh[direction]
                message += "\nfound in {} in [mesh] in the input file.".\
                    format(direction)
                raise RuntimeError(message)

        return n_points


if __name__ == "__main__":

    print(("\n\nTo find out about the bout_runners, please read the user's "
           "manual, or have a look at 'BOUT/examples/bout_runners_example', "
           "or have a look at the documentation"))
