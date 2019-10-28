"""
Temporary file used to get an overview over the previous bout_runners
"""

# Preporatory
# FIXME: Change shell with subprocess
import os


# Core
    def _run_driver(self, combination, run_no):
        """
        The machinery which actually performs the runs.
        """

        # Get the time when the run starts
        start = datetime.datetime.now()
        # Do the run
        output, run_time = self._single_run(combination)
        # Print info to the log file for the runs
        self._append_run_log(start, run_no, run_time)
        print("\n")

    def _single_run(self, combination):
        """Makes a single MPIRUN of the program"""

        # Get the command to be used
        command = self._get_command_to_run(combination)

        # Time how long the time took
        tic = timeit.default_timer()

        # Launch the command
        status, out = launch(command,
                             runcmd=self._MPIRUN,
                             nproc=self._nproc,
                             pipe=True,
                             verbose=True)

        # If the run returns an exit code other than 0
        if status != 0:
            message = "! An error occurred. Printing the output to stdout !"
            print("{0}{1}{2}{1}{0}{3}".
                  format("\n", "!" * len(message), message, out))
            self._errors.append("RuntimeError")
            message = ("An error occurred the run."
                       " Please see the output above for details.")
            # Search if parantheses are present, but without ' or "
            if ("(" in combination and
                    not(re.search(r'\"(.*)\(', combination)
                        or re.search(r"\'(.*)\(", combination)))\
                or (")" in combination and
                    not(re.search(r'\)(.*)\"', combination)
                        or re.search(r"\)(.*)\'", combination))):
                message = (
                    "A '(' and/or ')' symbol seem to have appeared in the"
                    " command line.\nIf this true, you can avoid"
                    " this problem by adding an extra set of"
                    " quotation marks. For example\n\n"
                    "additional=('variable', 'bndry_xin',"
                    " '\"dirichlet_o4(0.0)\")'\n"
                    "rather than\n"
                    "additional=('variable', 'bndry_xin',"
                    " 'dirichlet_o4(0.0))'")
            else:
                message = ("An error occurred the run."
                           " Please see the output above for details.")
            raise RuntimeError(message)

        # Estimate elapsed time
        toc = timeit.default_timer()
        elapsed_time = toc - tic

        return out, elapsed_time

# CLI wrapper

    def __init__(self,
                 nproc=1,
                 directory="data",
                 prog_name=None,
                 solver=None,
                 mms=None,
                 atol=None,
                 rtol=None,
                 mxstep=None,
                 grid_file=None,
                 nx=None,
                 ny=None,
                 nz=None,
                 zperiod=None,
                 zmin=None,
                 zmax=None,
                 dx=None,
                 dy=None,
                 dz=None,
                 MXG=None,
                 MYG=None,
                 NXPE=None,
                 ixseps1=None,
                 ixseps2=None,
                 jyseps1_1=None,
                 jyseps1_2=None,
                 jyseps2_1=None,
                 jyseps2_2=None,
                 symGlobX=None,
                 symGlobY=None,
                 ddx_first=None,
                 ddx_second=None,
                 ddx_upwind=None,
                 ddx_flux=None,
                 ddy_first=None,
                 ddy_second=None,
                 ddy_upwind=None,
                 ddy_flux=None,
                 ddz_first=None,
                 ddz_second=None,
                 ddz_upwind=None,
                 ddz_flux=None,
                 nout=None,
                 timestep=None,
                 additional=None,
                 series_add=None,
                 restart=None,
                 restart_from=None,
                 redistribute=None,
                 use_expand=False,
                 max_proc=None,
                 intrp_method=None,
                 add_noise=None,
                 cpy_source=None,
                 cpy_grid=None,
                 sort_by=None,
                 make=None,
                 allow_size_modification=False):
        """
        basic_runner constructor
        ------------------------

        All the member data is set to None by default. If the
        data members are not set, the values from BOUT.inp will be used.
        The exception is nproc (default = 1), directory (default =
        "data"), use_expand (default = False) and
        allow_size_modification (default = False), which always needs to
        be set.

        Parameters
        ----------
        nproc : int
            Number of processors to use in the mpirun
        directory : str
            The directory of the BOUT.inp file
        prog_name : str or iterable
            Name of the excecutable. If none is set the name will be set from
            the *.o file.
        solver : str or iterable
            The solver to be used in the runs
        mms : bool
            Whether or not mms should be run
        atol : number or iterable
            Absolute tolerance
        rtol : number or iterable
            Relative tolerance
        mxstep : int or iterable
            Max internal step pr output step
        grid_file : str or iterable
            The grid file
        nx : int or iterable
            Number of nx in the run
        ny : int or iterable
            Number of ny in the run
        nz : int or iterable
            Number of nz in the run
        zperiod : int or iterable
            Domain size in  multiple of fractions of 2*pi
        zmin : number
            Minimum range of the z domain
        zmax : number
            Maximum range of the z domain
        dx : number or iterable
            Grid size in the x direction
        dy : number or iterable
            Grid size in the x direction number or iterable
        dz : number or iterable
            Grid size in the x direction number or iterable
        MXG : int or iterable
            The number of guard cells in the x direction
        MYG : int or iterable
            The number of guard cells in the y direction
        NXPE : int or iterable
            Numbers of processors in the x direction
        ixseps1 : int or iterable
            Separatrix location for "upper" divertor
        ixseps2 : int or iterable
            Separatrix location for "lower" divertor
        jyseps1_1 : int or iterable
            Branch cut location 1_1 (see user's manual for details)
        jyseps1_2 : int or iterable
            Branch cut location 1_2 (see user's manual for details)
        jyseps2_1 : int or iterable
            Branch cut location 2_1 (see user's manual for details)
        jyseps2_2 : int or iterable
            Branch cut location 2_2 (see user's manual for details)
        symGlobX : bool
            Whether or not to use symmetricGLobalX (x defined
            symmetrically between 0 and 1)
        symGlobY : bool
            Whether or not to use symmetricGLobalY (y defined
            symmetrically)
        ddx_first : str or iterable
            Method used for for first ddx terms
        ddx_second : str or iterable
            Method used for for second ddx terms
        ddx_upwind : str or iterable
            Method used for for upwind ddx terms
        ddx_flux : str or iterable
            Method used for for flux ddx terms
        ddy_first : str or iterable
            Method used for for first ddy terms
        ddy_second : str or iterable
            Method used for for second ddy terms
        ddy_upwind : str or iterable
            Method used for for upwind ddy terms
        ddy_flux : str or iterable
            Method used for for flux ddy terms
        ddz_first : str or iterable
            Method used for for first ddz terms
        ddz_second : str or iterable
            Method used for for second ddz terms
        ddz_upwind : str or iterable
            Method used for for upwind ddz terms
        ddz_flux : str or iterable
            Method used for for flux ddz terms
        nout : int or iterable
            Number of outputs stored in the *.dmp.* files
        timestep : int or iterable
            The time between each output stored in the *.dmp.* files
        additional : tuple or iterable
            Additional option for the run given on the form

            >>> ("section_name","variable name", values)

            or as iterable on the same form, where values can be any
            value or string or an iterable of those
        series_add : tuple or iterable
            The same as above, with the exception that no combination
            will be performed between the elements during a run
        restart : str
            Wheter or not to use the restart files. Must be either
            "overwrite" or "append" if set
        restart_from : [str | function]
            Path to restart if string. If function: A function which
            takes the current dmp_folder and kwargs (given to
            execute_runs) as input and returns the restart path. The
            function is handy when restarting from jobs while doing a
            parameter scan.
        redistribute : int
            The number of processors the redistribute the restart files
            to. Calls the redistribute function in boutdata.restart.
            Will only be effective if "restart" is not None
        use_expand : bool
            Only used when restarting.
            If there is a mismatch in nz between the requested nz and
            the nz found in the restart file, boutdata.restart.resizeZ
            will be used if use_expand = True, if not
            boutdata.restart.resize will be used
        max_proc : int
            Only used when restarting.
            Max processors used when calling boutdata.restart.resize
        intrp_method: str
            Only used when restarting, and when the mesh is resizied.
            Sets the method used in the interpolation.
        add_noise : dict
            Adding noise to the restart files by calling the addnoise
            function in boutdata.restart.  Will only be effective if
            "restart" is not None.  Must be given as a dict with "var"
            and 'scale" as keys if used.  The value of "var" must be a
            string or None.  If set to None, then all the evolved
            variables will be added noise to.  The value of "scale" will
            be the scale of the noise, if set to None the default value
            will be used.
            Example:

            >>> add_noise = {"n":1e-4, "Te":1e-5}

        cpy_source : bool
            Wheter or not to copy the source files to the folder of the
            *.dmp.* files
        cpy_grid : bool
            Whether or not to copy the grid files to the folder of the
            *.dmp.* files
        sort_by : str
            Defining what will be the fastest running variable in the
            run, which can be useful if one for example would like to
            "group" the runs before sending it to a post processing
            function (see the docstring of the run function for more
            info). The possibilities are

                * "spatial_domain"
                * "temporal_domain"
                * "solver"
                * "ddx_first"
                * "ddx_second"
                * "ddx_upwind"
                * "ddx_flux"
                * "ddy_first"
                * "ddy_second"
                * "ddy_upwind"
                * "ddy_flux"
                * "ddz_first"
                * "ddz_second"
                * "ddz_upwind"
                * "ddz_flux"
                * Any "variable_name" from additional or series_add
                * An iterable consisting of several of these.

            If an iterable is given, then the first element is going to
            be the fastest varying variable, the second element is going
            to be the second fastest varying variable and so on.
        make : bool
            Whether or not to make the program
        allow_size_modification : bool
            Whether or not to allow bout_runners modify nx and ny in
            order to find a valid split of the domain
        """

        # Setting the member data
        self._nproc = nproc
        self._directory = directory
        self._solver = self._set_member_data(solver)
        self._mms = mms
        self._atol = self._set_member_data(atol)
        self._rtol = self._set_member_data(rtol)
        self._mxstep = self._set_member_data(mxstep)
        self._grid_file = self._set_member_data(grid_file)
        self._nx = self._set_member_data(nx)
        self._ny = self._set_member_data(ny)
        self._nz = self._set_member_data(nz)
        self._zperiod = self._set_member_data(zperiod)
        self._zmin = self._set_member_data(zmin)
        self._zmax = self._set_member_data(zmax)
        self._dx = self._set_member_data(dx)
        self._dy = self._set_member_data(dy)
        self._dz = self._set_member_data(dz)
        self._MXG = MXG
        self._MYG = MYG
        self._NXPE = self._set_member_data(NXPE)
        self._ixseps1 = self._set_member_data(ixseps1)
        self._ixseps2 = self._set_member_data(ixseps2)
        self._jyseps1_1 = self._set_member_data(jyseps1_1)
        self._jyseps1_2 = self._set_member_data(jyseps1_2)
        self._jyseps2_1 = self._set_member_data(jyseps2_1)
        self._jyseps2_2 = self._set_member_data(jyseps2_2)
        self._symGlobX = symGlobX
        self._symGlobY = symGlobY
        self._ddx_first = self._set_member_data(ddx_first)
        self._ddx_second = self._set_member_data(ddx_second)
        self._ddx_upwind = self._set_member_data(ddx_upwind)
        self._ddx_flux = self._set_member_data(ddx_flux)
        self._ddy_first = self._set_member_data(ddy_first)
        self._ddy_second = self._set_member_data(ddy_second)
        self._ddy_upwind = self._set_member_data(ddy_upwind)
        self._ddy_flux = self._set_member_data(ddy_flux)
        self._ddz_first = self._set_member_data(ddz_first)
        self._ddz_second = self._set_member_data(ddz_second)
        self._ddz_upwind = self._set_member_data(ddz_upwind)
        self._ddz_flux = self._set_member_data(ddz_flux)
        self._nout = self._set_member_data(nout)
        self._timestep = self._set_member_data(timestep)
        self._additional = additional
        self._series_add = series_add
        self._restart = restart
        self._restart_from = restart_from
        self._redistribute = redistribute
        self._use_expand = use_expand
        self._max_proc = max_proc
        self._intrp_method = intrp_method
        self._add_noise = add_noise
        self._cpy_source = cpy_source
        self._cpy_grid = cpy_grid
        self._sort_by = self._set_member_data(sort_by)
        self._make = make
        self._allow_size_modification = allow_size_modification

        # Make some space to distinguish from the rest of the terminal
        print("\n")

        # Initializing self._warnings and self._error
        # self._warnings will be filled with warnings
        # self._errors will be filled with errors
        # The warnings and errors will be printed when the destructor is called
        self._warnings = []
        self._errors = []

        # Check if make is a boolean
        if self._make is not None:
            if not isinstance(self._make, bool):
                self._errors.append("TypeError")
                raise TypeError("make must be boolean if set")

        # Set self._program_name
        self._set_program_name(prog_name)

        # Make the file if make is True
        if self._make:
            _run_make(self._errors)

        # Obtain the MPIRUN
        self._MPIRUN = getmpirun()

        # The run type is going to be written in the run.log file
        self._run_type = "basic"

        # self._additional must be on a special form (see
        # basic_error_checker).
        if self._additional is not None:
            if not(hasattr(self._additional, "__iter__")) or\
               (isinstance(self._additional, str)) or\
               (isinstance(self._additional, dict)):
                # Put additional as a double iterable
                self._additional = ((self._additional),)
            else:
                if not(hasattr(self._additional[0], "__iter__")) or\
                   (isinstance(self._additional[0], str)) or\
                   (isinstance(self._additional, dict)):
                    # Put self._additional as an iterable
                    self._additional = (self._additional,)
        # Do the same for series_add
        if self._series_add is not None:
            if not(hasattr(self._series_add, "__iter__")) or\
               (isinstance(self._series_add, str)) or\
               (isinstance(self._series_add, dict)):
                # Put series_add as a double iterable
                self._series_add = ((self._series_add),)
            else:
                if not(hasattr(self._series_add[0], "__iter__")) or\
                   (isinstance(self._series_add[0], str)) or\
                   (isinstance(self._series_add, dict)):
                    # Put self._series_add as an iterable
                    self._series_add = (self._series_add,)

        # Check that nproc is given correctly
        if not isinstance(self._nproc, int):
            message = ("nproc is of wrong type\n"
                       "nproc must be given as an int")
            self._errors.append("TypeError")
            raise TypeError(message)

        if self._NXPE is not None:
            # Make self._NYPE as an appendable list
            self._NYPE = []

            # Check that NXPE is of correct type
            check_if_int = (
                (self._NXPE, "NXPE"),
            )
            _check_for_correct_type(self._errors, var=check_if_int,
                                    the_type=int,
                                    allow_iterable=True)

            # Check that NXPE and nproc is consistent
            for cur_NXPE in self._NXPE:
                if (self._nproc % cur_NXPE) != 0:
                    self._errors.append("RuntimeError")
                    message = "nproc =" + str(self._nproc) +\
                              " not divisible by" +\
                              " NXPE = " + str(cur_NXPE) +\
                              " (the number of processors in the x direction)"
                    raise RuntimeError(message)

                # Append NYPE
                self._NYPE.append(int(self._nproc / cur_NXPE))
        else:
            self._NYPE = None

        # Check if the instance is set correctly
        self._check_for_basic_instance_error()

        # We need to find options in BOUT.inp. We use BOUTOption for this
        # Object initialization
        self._inputFileOpts = BOUTOptions(self._directory)
        # Convert indices to lowercase
        self._inputFileOpts.root = dict(
            (key.lower(), value) for key, value in self._inputFileOpts.root.items())
        self._inputFileOpts.mesh = dict(
            (key.lower(), value) for key, value in self._inputFileOpts.mesh.items())

        # Initialize outputs from execute runs
        self._PBS_id = []
        self._dmp_folders = []

    def __del__(self):
        """The destructor will print all the warning and error messages"""

        # Switch to see if error occured
        error_occured = False

        # If errors occured
        if len(self._errors) > 0:
            message = "! A {} occurred. !".format(self._errors[0])
            # Find the boarder length
            len_boarder = len(message)
            # Print the message
            print("{0}{1}\n{2}\n{1}{0}".
                  format("\n" * 2, "!" * len_boarder, message))
            error_occured = True
        if len(self._warnings) > 0:
            print("{}The following WARNINGS were detected:\n{}".
                  format("\n" * 3, "-" * 80))
            for warning in self._warnings:
                print(warning + "\n")
            print("{}{}".format("-" * 80, "\n" * 3))
        elif len(self._warnings) > 0 and not(error_occured):
            print("{} {}".format("\n" * 3, "~" * 69))
            print(("| No WARNINGS detected before instance destruction in "
                   "'bout_runners'. |"))

    def _get_command_to_run(self, combination):
        """
        Returns a string of the command which will run the BOUT++
        program
        """

        # Creating the arguments
        arg = " -d {} {}".format(self._dmp_folder, combination)

        # If the run is set to overwrite
        if self._restart == "overwrite":
            arg += " restart"
        elif self._restart == "append":
            arg += " restart append"

        # Replace excessive spaces with a single space
        arg = " ".join(arg.split())
        command = "./{} {}".format(self._program_name, arg)

        return command

# Bundle generator
    def _get_possibilities(self):
        """
        Returns the list of the possibilities. In get_combinations
        the elements of this list is going to be put together to a list
        of strings which will be used when making a run.
        """

        # Appendable list
        spatial_grid_possibilities = []
        if (self._grid_file is None):
            # Dictionary where
            # - the first element is the variable itself
            # - the second element is the section of the variable
            # - the third element is an appendable list
            spatial_grid_str = {
                "nx": (self._nx, "mesh:", []),
                "ny": (self._ny, "mesh:", []),
                "nz": (self._nz, "mesh:", []),
                "dx": (self._dx, "mesh:", []),
                "dy": (self._dy, "mesh:", []),
                "dz": (self._dz, "mesh:", []),
                "zperiod": (self._zperiod, "", []),
                "zmin": (self._zmin, "", []),
                "zmax": (self._zmax, "", []),
            }
            # Store the keys as an own variable
            keys = tuple(spatial_grid_str.keys(), )
            # Append the different dimension to the list of strings
            for key in keys:
                # If the variable is not empty
                if spatial_grid_str[key][0] is not None:
                    # Fill the appendable list with the elements from
                    # the variable
                    for elem in spatial_grid_str[key][0]:
                        spatial_grid_str[key][2].append(
                            "{}{}={}".
                            format(spatial_grid_str[key][1], key, elem)
                        )

            # The goal is to combine the these strings to one string
            # Find the largest length
            lengths = tuple(len(spatial_grid_str[key][2]) for key in keys)
            max_len = np.max(lengths)
            # Make the strings the same length
            for key in keys:
                # We do this by filling it with empty strings
                while len(spatial_grid_str[key][2]) <= max_len:
                    spatial_grid_str[key][2].append("")

            # Append this to the spatial grid possibilities as a string
            for number in range(max_len):
                # Make a tuple
                current_grid = tuple(spatial_grid_str[key][2][number]
                                     for key in keys)
                # Join the strings in the list and append
                spatial_grid_possibilities.append(" ".join(current_grid))

        # Appendable lists
        temporal_grid_possibilities = []
        timestep_str = []
        nout_str = []
        # Append the different time options to the list of strings
        if self._timestep is not None:
            for timestep in self._timestep:
                timestep_str.append("timestep={}".format(timestep))
        if self._nout is not None:
            for nout in self._nout:
                nout_str.append("nout={}".format(nout))
        # Combine the strings to one string
        # Find the largest length
        max_len = np.max([len(timestep_str), len(nout_str)])
        # Make the strings the same length
        if len(timestep_str) < max_len:
            timestep_str.append("")
        if len(nout_str) < max_len:
            nout_str.append("")
        # Append the temporal grid possibilities as a string
        for number in range(max_len):
            # Make a tuple
            current_times = (timestep_str[number],
                             nout_str[number]
                             )
            # Join the strings in the list and append
            temporal_grid_possibilities.append(" ".join(current_times))

        # Appendable list
        series_add_possibilities = []
        if self._series_add is not None:
            # Dictionary to handle the data, where the key is going to
            # be the element number in self._series_add, and the values
            # are going to be the sub dictionary defined below
            all_info = {}
            # Loop through all elements and fill the dictionary
            for nr, elem in enumerate(self._series_add):
                # Put in the sub dictionary
                all_info[nr] = {"values": None,
                                "section_and_var": None,
                                "sec_var_vals": []}
                # Fill the values
                all_info[nr]["values"] = elem[2]
                # Fill the section and variable key
                all_info[nr]["section_and_var"] = "{}:{}=".\
                                                  format(elem[0], elem[1])
                # Fill in the combinations
                for val in all_info[nr]["values"]:
                    all_info[nr]["sec_var_vals"].append(
                        all_info[nr]["section_and_var"] + str(val)
                    )

            # Make an appendable list
            all_sec_var_vals = []
            for key in all_info.keys():
                all_sec_var_vals.append(all_info[key]["sec_var_vals"])

            # Zip the sec_var_vals together (* unpacks), join them with
            # a space, and append them to series_add_possibilities
            for one_possibility in zip(*all_sec_var_vals):
                series_add_possibilities.append(" ".join(one_possibility))

        # This makes the member data iterable, and usable in
        # generate_possibilities
        if self._MXG is not None:
            self._MXG = (self._MXG,)
        if self._MYG is not None:
            self._MYG = (self._MYG,)
        if self._mms is not None:
            self._mms = (self._mms,)
        if self._symGlobX is not None:
            self._symGlobX = (self._symGlobX,)
        if self._symGlobY is not None:
            self._symGlobY = (self._symGlobY,)

        tuple_of_variables = [
            (self._solver, "solver", "type"),
            (self._mms, "solver", "mms"),
            (self._atol, "solver", "atol"),
            (self._rtol, "solver", "rtol"),
            (self._mxstep, "solver", "mxstep"),
            (self._MXG, "", "MXG"),
            (self._MYG, "", "MYG"),
            (self._NXPE, "", "NXPE"),
            (self._NYPE, "", "NYPE"),
            (self._grid_file, "", "grid"),
            (self._ddx_first, "ddx", "first"),
            (self._ddx_second, "ddx", "second"),
            (self._ddx_upwind, "ddx", "upwind"),
            (self._ddx_flux, "ddx", "flux"),
            (self._ddy_first, "ddy", "first"),
            (self._ddy_second, "ddy", "second"),
            (self._ddy_upwind, "ddy", "upwind"),
            (self._ddy_flux, "ddy", "flux"),
            (self._ddz_first, "ddz", "first"),
            (self._ddz_second, "ddz", "second"),
            (self._ddz_upwind, "ddz", "upwind"),
            (self._ddz_flux, "ddz", "flux"),
            (self._ixseps1, "mesh", "ixseps1"),
            (self._ixseps2, "mesh", "ixseps2"),
            (self._jyseps1_1, "mesh", "jyseps1_1"),
            (self._jyseps1_2, "mesh", "jyseps1_2"),
            (self._jyseps2_1, "mesh", "jyseps2_1"),
            (self._jyseps2_2, "mesh", "jyseps2_2"),
            (self._symGlobX, "mesh", "symmetricGlobalX"),
            (self._symGlobY, "mesh", "symmetricGlobalY")
        ]

        if self._additional is not None:
            for additional in self._additional:
                # If the last element of additional is not iterable we need
                # put them into a tuple to make them iterable (in order to
                # use them in generate_possibilities)
                if (not(hasattr(additional[2], "__iter__"))) or\
                   (isinstance(additional[2], str)):
                    # We have to specify the whole additional, as this can
                    # be given as a tuple, and tuples does not support item
                    # assignments
                    additional = (additional[0],
                                  additional[1],
                                  (additional[2],))
                # Append the additional to tuple of variables
                tuple_of_variables.append(
                    (additional[2],
                     additional[0],
                     additional[1])
                )

        # Start out with the already generated
        # spatial_grid_possibilities and temporal_grid_possibilities
        list_of_possibilities = [spatial_grid_possibilities,
                                 temporal_grid_possibilities,
                                 series_add_possibilities]

        # Append the possibilities to the list of possibilities
        for var in tuple_of_variables:
            list_of_possibilities.append(
                self._generate_possibilities(var[0], var[1], var[2])
            )

        # Return the list_of possibilities
        return list_of_possibilities

    def _get_combinations(self, input_list):
        """
        The input_list is a list with lists as element.
        Returns a list of all combinations between the elements of the
        input_list.
        """

        # Remove empty elements in input_list in order for
        # itertools.product to work
        input_list = [elem for elem in input_list if elem != []]

        # If we would like to sort the input list (choose which variable
        # to be the fastest varying)
        if self._sort_by is not None:
            # Swap the list corresponding to the sort_by statement so
            # that that list will be the last. The itertools.product
            # will then make that list the fastest varying in the list
            input_list = self._get_swapped_input_list(input_list)
        else:
            # Initialize this member data to None
            self._len_group = None

        # The last element in the input_list will be the fastest varying
        # element
        all_combinations_as_tuple = list(itertools.product(*input_list))

        # all_combination_as_tuple is a list with tuples as elements
        # We would like to combine the elements in these tuples to one
        # string
        # Make an appendable list
        all_combinations_as_strings = []

        # Loop over the elements in the list containing tuples
        for a_tuple in all_combinations_as_tuple:
            # Join the elements in a tuple and store it
            all_combinations_as_strings.append(" ".join(a_tuple))

        return all_combinations_as_strings

    def _generate_possibilities(self, variables=None, section=None, name=None):
        """Generate the list of strings of possibilities"""

        if variables is not None:
            # Set the section name correctly
            if section != "":
                section = section + ":"
            else:
                section = ""
            # Set the combination of the variable
            var_possibilities = []
            # Find the number of different dimensions

            for var in variables:
                var_possibilities.append("{}{}={}".format(section, name, var))
        else:
            var_possibilities = []

        return var_possibilities

    def _prepare_dmp_folder(self, combination, **kwargs):
        """
        Prepare the dump folder for runs

        - Obtain the folder name to restart from
        - Obtain folder name and copy the input file to the final folder.
        - Check if restart files are present if restart is set (set
          restart to None if not found).
        - Find appropriate mxg and myg if redistribute is set.
        - Copy restart files if restart_from and/or redistribute is set
        - Redistribute restart files if redistribute and restart is set
        - Resize the runs (change nx, ny and/or nz) if the dimension is
          changed.
        - resizeZ if nz is set and it deviates from what is found in the
          restart files.
        - Add noise to the restart files if add_noise and restart is set
        - Copy files if restart is set to overwrite
        - Copy the source files to the final folder is cpy_source is True.

        Parameters
        ----------
        combination : sequence (not str)
            The current combination to be run
        **kwargs : any
            Extra parameters given to self._restart_from from function (if
            any)

        Returns do_run = False if there are any troubles with the copying
        """

        # do_run is set to True by default
        do_run = True

        folder_name = self._get_folder_name(combination)
        self._dmp_folder = os.path.join(self._directory, folder_name)
        # If the last character is "/", then remove it
        if self._dmp_folder[-1] == "/":
            self._dmp_folder = self._dmp_folder[:-1]

        # Create folder if it doesn't exists
        _create_folder(self._dmp_folder)

        if not isinstance(self._dmp_folders, tuple):
            # If self._dmp_folders is a tuple, it means that execute runs
            # is called more then once.
            # self._dmp_folders should then not be appended
            self._dmp_folders.append(self._dmp_folder)

        # If self._dmp_folder contains anything other than
        # self._directory
        if self._dmp_folder != self._directory:
            # Copy the input file into this folder
            src = os.path.join(self._directory, "BOUT.inp")
            shutil.copy2(src, self._dmp_folder)

        if self._restart_from is not None:

            if isinstance(self._restart_from, str):
                self._cur_restart_from = self._restart_from
            elif hasattr(self._restart_from, "__call__"):
                self._cur_restart_from =\
                    self._restart_from(self._dmp_folder, **kwargs)
                if not isinstance(self._cur_restart_from, str):
                    message = ("The restart_from from function must "
                               "return a string")
                    raise ValueError(message)

            # Check if any restart files are present
            # This check is performed after waiting for other runs to finish
            if len(glob.glob(
                    os.path.join(self._cur_restart_from, "*restart*"))) == 0:
                self._errors.append("FileNotFoundError")
                raise FileNotFoundError("No restart files found in " +
                                        self._cur_restart_from)

        else:
            self._cur_restart_from = None

        dmp_files = glob.glob(os.path.join(self._dmp_folder, "*.restart.*"))
        # If no dump files are found, set restart to "None"
        if len(dmp_files) == 0 and\
           self._restart is not None and\
           self._cur_restart_from is None:
            message = ("'restart' was set to {}"
                       ", but no restart files found."
                       " Setting 'restart' to None").format(self._restart)
            self._restart = None
            _warning_printer(message)
            self._warnings.append(message)

        if self._redistribute:
            redistribute_MXG, redistribute_MYG = self._get_MXG_MYG()

        if self._restart and self._cur_restart_from:
            if self._redistribute:
                # Use the redistribute function to copy the restart file
                do_run = self._check_if_run_already_performed(
                    restart_file_search_reason="redistribute")

                if do_run:
                    print("\nCopying files from {0} to {1}\n".
                          format(self._cur_restart_from, self._dmp_folder))
                    do_run = redistribute(self._redistribute,
                                          path=self._cur_restart_from,
                                          output=self._dmp_folder,
                                          mxg=redistribute_MXG,
                                          myg=redistribute_MYG,
                                          )
                    if not do_run:
                        message = "Redistribute failed, run skipped"
                        _warning_printer(message)
                        self._warnings.append(message)
            else:
                # Copy the files to restart
                do_run = self._copy_run_files()

        elif self._restart and self._redistribute:
            # Save the files from previous runs
            dst = self._move_old_runs(folder_name="redistribute",
                                      include_restart=True)

            do_run = redistribute(self._redistribute,
                                  path=dst,
                                  output=self._dmp_folder,
                                  mxg=redistribute_MXG,
                                  myg=redistribute_MYG,
                                  )

        # NOTE: This is already done if self._redistribute is set
        if self._restart == "overwrite" and not(self._redistribute) and do_run:
            self._move_old_runs(folder_name="restart",
                                include_restart=False)

        if self._restart and do_run:
            if self._nz:
                # The current nz should be in the second index as any
                # eventual other names would come from additional or
                # series_add
                cur_nz = int(self._dmp_folder.
                             split("nz")[1].
                             split("/")[0].
                             replace("_", ""))
            else:
                # The nz size is not changed, will use the one from
                # the input file
                try:
                    cur_nz = self._get_dim_from_input("nz")
                except KeyError:
                    cur_nz = self._get_dim_from_input("mz")

            # Make sure cur_nz is divisible by 2 if cur_nz != 1
            if cur_nz != 1:
                if cur_nz % 2 != 0:
                    old_cur_nz = cur_nz
                    cur_nz += 1
                    if cur_nz % 2 != 0:
                        cur_nz = old_cur_nz - 1
                    else:
                        message = "nz = {} not a power of 2".format(cur_nz)
                        raise RuntimeError(message)

        # Flag to check if the mesh has been resized
        resized = False

        if self._restart and do_run and (self._nx or self._ny or self._nz):
            # Obtain MXG and MYG
            MXG, MYG = self._get_MXG_MYG()

            # Checking if the sizes are changed
            # Finding the current sizes
            # The current sizes should be in the second index as any
            # eventual other names would come from additional or
            # series_add
            # Finding nx
            if self._nx:
                cur_nx = int(self._dmp_folder.
                             split("nx")[1].
                             split("/")[0].
                             split("_")[1])
            else:
                # The nx size is not changed, will use the one from
                # the input file
                cur_nx = self._get_dim_from_input("nx")
            # Finding ny
            if self._ny:
                cur_ny = int(self._dmp_folder.
                             split("ny")[1].
                             split("/")[0].
                             split("_")[1])
            else:
                # The ny size is not changed, will use the one from
                # the input file
                cur_ny = self._get_dim_from_input("ny")

            # Finding the sizes in the restart files
            file_name = glob.glob(
                os.path.join(self._dmp_folder, "BOUT.restart.0.*"))[0]

            with DataFile(file_name) as f:
                # Loop over the variables in the file
                NYPE = f.read("NYPE")
                NXPE = f.read("NXPE")
                for var in f.list():
                    # Read the data
                    data = f.read(var)

                    # Find 3D variables
                    if f.ndims(var) == 3:
                        local_nx, local_ny, nz = data.shape
                        MXSUB = local_nx - 2 * MXG
                        MYSUB = local_ny - 2 * MYG
                        nx = NXPE * MXSUB + 2 * MXG
                        ny = NYPE * MYSUB

                        if nx == cur_nx and ny == cur_ny and nz == cur_nz:
                            call_resize = False
                            break
                        elif nx == cur_nx and ny == cur_ny and nz != cur_nz:
                            if nz == 1:
                                # Override user specification to save time
                                self._use_expand = True
                            if self._use_expand:
                                call_resize = False
                            else:
                                call_resize = True
                            break
                        else:
                            call_resize = True
                            if self._restart == "append":
                                message = ("Cannot change nx, ny and/or nz "
                                           "when appending\n")
                                # Extra plane in nz
                                message += (
                                    "Requested nx = {}, nx in restart file = {}\n"
                                    "Requested ny = {}, ny in restart file = {}\n"
                                    "Requested nz = {}, nz in restart file = {}\n"
                                    "Resizing:\n"). format(
                                    cur_nx, nx, cur_ny, ny, cur_nz, nz)
                                raise IOError(message)
                            else:
                                break

            if call_resize:
                # Move runs
                dst = self._move_old_runs(folder_name="beforeResize",
                                          include_restart=True)

                # Redistributing the data to one file
                # Redistribute
                success = redistribute(1,
                                       path=dst,
                                       output=self._dmp_folder,
                                       mxg=MXG,
                                       myg=MYG,
                                       )
                if not success:
                    message = ("Failed to redistribute to one file when "
                               "resizing evolved variables")
                    raise RuntimeError(message)

                # Move the redistributed to the resize folder
                file_name = glob.glob(os.path.join(self._dmp_folder,
                                                   "BOUT.restart.0.*"))[0]
                path, name = os.path.split(file_name)
                before_resize_dir = os.path.join(path, "beforeResizingOneFile")
                _create_folder(before_resize_dir)
                shutil.move(file_name, before_resize_dir)

                if self._use_expand:
                    print("\nDimension change found:\n"
                          "Requested nx = {}, nx in restart file = {}\n"
                          "Requested ny = {}, ny in restart file = {}\n"
                          "Resizing:\n"
                          .format(cur_nx, nx, cur_ny, ny))
                    the_nz = nz
                else:
                    print("\nDimension change found:\n"
                          "Requested nx = {}, nx in restart file = {}\n"
                          "Requested ny = {}, ny in restart file = {}\n"
                          "Requested nz = {}, nz in restart file = {}\n"
                          "Resizing:\n"
                          .format(cur_nx, nx, cur_ny, ny, cur_nz, nz))
                    the_nz = cur_nz

                # NOTE: Different definition for nx and ny
                success = resize(cur_nx, cur_ny + 2 * MYG, the_nz,
                                 mxg=MXG,
                                 myg=MYG,
                                 path=before_resize_dir,
                                 output=self._dmp_folder,
                                 method=self._intrp_method,
                                 maxProc=self._max_proc)
                print("\n")

                if not success:
                    do_run = False
                    if self._cur_restart_from:
                        print("Something went wrong: Reomving {}\n".
                              format(os.path.split(dst)[0], "\n"))
                        shutil.rmtree(os.path.split(dst)[0])
                    message = "Resize failed, skipping run."
                    self._warnings.append(message)
                    _warning_printer(message)

                # Move the resized restart file
                path, name = os.path.split(file_name)
                # Create a temporary file which "redistribute" can read
                # from
                after_resize_dir = os.path.join(path, "afterResizingOneFile")
                _create_folder(after_resize_dir)
                shutil.move(file_name, after_resize_dir)

                # Redistribute to original split
                if self._redistribute:
                    nproc = self._redistribute
                else:
                    nproc = self._nproc

                success = redistribute(nproc,
                                       path=after_resize_dir,
                                       output=self._dmp_folder,
                                       mxg=MXG,
                                       myg=MYG,
                                       )

                if not success:
                    message = ("Failed to redistribute after "
                               "resizing evolved variables")
                    if self._cur_restart_from:
                        print("Something went wrong: Reomving {}\n".
                              format(os.path.split(dst)[0], "\n"))
                        shutil.rmtree(os.path.split(dst)[0])
                    raise RuntimeError(message)

                resized = True

        if self._restart and do_run\
           and self._nz and not resized and self._use_expand:
            # The current nz should be in the second index as any
            # eventual other names would come from additional or
            # series_add
            cur_nz = int(self._dmp_folder.
                         split("nz")[1].
                         split("/")[0].
                         split("_")[1])
            if self._restart == "append":
                # Check if nz is the same as in the restart files
                # Start by opening the 0th restart file
                file_name = glob.glob(os.path.join(self._dmp_folder,
                                                   "BOUT.restart.0.*"))[0]
                with DataFile(file_name) as f:
                    # Loop over the variables in the file
                    for var in f.list():
                        # Read the data
                        data = f.read(var)

                        # Find 3D variables
                        if f.ndims(var) == 3:
                            _nx, _ny, nz = data.shape

                            if nz != cur_nz:
                                message = ("Cannot change nz when appending\n"
                                           "nz in restart file = {}\n"
                                           "current run nz = {}").\
                                    format(nz, cur_nz)
                                raise IOError(message)
                            else:
                                break

            # Get the folder of the restart files
            elif resized:
                # Copy the files to afterResizeRedistr
                after_resize_dir = os.path.join(path, "afterResizeRedistr")
                _create_folder(after_resize_dir)
                file_names = glob.glob(
                    os.path.join(self._dmp_folder, "BOUT.restart.*"))
                for file_name in file_names:
                    shutil.copy2(file_name, after_resize_dir)
                # The restart files are stored in the resize folder
                folder = "afterResizeRedistr*"
            elif self._restart == "overwrite" and not(self._redistribute):
                # The restart files are stored in the restart folder
                folder = "restart*"
            elif self._restart == "overwrite" and self._redistribute:
                if self._cur_restart_from:
                    _ = self._move_old_runs(folder_name="redistribute",
                                            include_restart=True)

                # The restart files are stored in the restart folder
                folder = "redistribute*"

            if self._restart == "overwrite":
                # Find the restart files
                location = sorted(
                    glob.glob(
                        os.path.join(
                            self._dmp_folder,
                            folder)))
                location = location[-1]

                # Check whether nz is changing or not
                file_name = glob.glob(
                    os.path.join(location, "BOUT.restart.0.*"))[0]

                with DataFile(file_name) as f:
                    # Loop over the variables in the file
                    for var in f.list():
                        # Read the data
                        data = f.read(var)

                        # Find 3D variables
                        if f.ndims(var) == 3:
                            nx, ny, nz = data.shape

                            if nz == cur_nz:
                                call_expand = False
                            else:
                                if nz < cur_nz:
                                    call_expand = True
                                else:
                                    if self._cur_restart_from:
                                        print(("Something went wrong: "
                                               "Reomving {}\n").
                                              format(os.path.split(location)[0]))
                                        shutil.rmtree(
                                            os.path.split(location)[0])
                                    message = ("Cannot decrease nz from {} to"
                                               " {} in a restart").\
                                        format(nz, cur_nz)
                                    raise IOError(message)

                if call_expand:
                    print("\nnz is bigger than in restart file, expanding:\n")
                    success = resizeZ(cur_nz,
                                      path=location,
                                      output=self._dmp_folder)
                    print("\n")

                    if not success:
                        do_run = False
                        if self._cur_restart_from:
                            print("Something went wrong: Reomving {}\n".
                                  format(os.path.split(location)[0]))
                            shutil.rmtree(os.path.split(location)[0])
                        message = "resizeZ failed, skipping run."
                        self._warnings.append(message)
                        _warning_printer(message)

        if self._restart and self._add_noise and do_run:
            print("Now adding noise\n")
            for var, scale in self._add_noise.items():
                if scale is None:
                    scale = 1e-5
                    print("No scale set for '{}', setting to {}\n".
                          format(var, scale))
                try:
                    addnoise(path=self._dmp_folder,
                             var=var,
                             scale=scale)
                except Exception as ex:
                    print("{0}{1}addnoise failed with the following error:{0}".
                          format("\n" * 4, "!" * 3))
                    raise ex
            print("\n")

        if self._cpy_source and do_run:
            # This will copy all C++ files to the dmp_folder
            cpp_extension = (".cc", ".cpp", ".cxx", ".C", ".c++",
                             ".h", ".hpp", ".hxx", ".h++")
            # Copy for all files in the extension
            for extension in cpp_extension:
                file_names = glob.glob("*" + extension)
                for a_file in file_names:
                    shutil.copy2(a_file, self._dmp_folder)

        return do_run

# Bundle handler

    def _print_run_or_submit(self):
        """Prints "Now running" """
        print("\nNow running:")

    def execute_runs(self,
                     job_dependencies=None,
                     remove_old=False,
                     post_processing_function=None,
                     post_process_after_every_run=False,
                     **kwargs):
        """
        Makes a run for each of the combination given by the member data.

        Parameters
        ----------
        job_dependencies : [None | sequence (not str)], default: None
            If the jobs should be run after other jobs. This input is
            only effective if the object calling the function is a
            PBS_runner.
        remove_old : bool, default : False
            Whether old run files should be deleted or not
        post_processing_function : callable
            A function to be called after one or several run. This
            function must accept the string of self._dmp_folder if
            post_process_after_each_run is True, and a tuple of dmp
            folders if post_process_after_each_run is False
        post_process_after_each_run : bool, default: False
            Boolean telling whether post_processing_function should be
            called after each run (if True), or after the number of runs
            decided by self._sort_by (see the constructor of
            basic_runner for more info)
        **kwargs : any
            Parameters to be passed to the post_processing_function and
            self._restart_from function (if any)

        Returns
        -------
        self._dmp_folders : sequence (not str)
            A sequence of the folder locations made from the runner
        self._PBS_id : sequence (not str)
            A sequence of the PBS ids is returned.
        """

        if self.__class__.__name__ == "PBS_runner":
            # Wait for jobs to finish
            if job_dependencies is not None:
                # Ensure that job_dependencies are just numbers
                job_dependencies = [int(re.match('\d+', j).group(0))
                                    for j in job_dependencies
                                    if re.match('\d+', j) is not None]
                if len(job_dependencies) != 0:
                    print("\nWill now wait for these jobs to finish\n{}\n".
                          format("\n".join([str(j) for j in job_dependencies])))
                while len(job_dependencies) > 0:
                    # Get current jobs
                    status, output = shell("qstat", pipe=True)
                    job_queue = output.split("\n")
                    # Find the jobIds
                    job_queue = [int(re.match('\d+', j).group(0))
                                 for j in job_queue
                                 if re.match('\d+', j) is not None]
                    # These jobs will be removed from job_dependencies
                    pop_jobs = []
                    for job in job_dependencies:
                        if job not in job_queue:
                            pop_jobs.append(job)

                    for job in pop_jobs:
                        job_dependencies.remove(job)

                    time.sleep(60)

        # Check for errors in the run function
        _error_check_for_run_input(self._errors, self._restart, remove_old,
                                   post_processing_function,
                                   post_process_after_every_run)

        # Create the run log
        self._create_run_log()

        # We check that the given combination of nx and ny is
        # possible to perform with the given nproc
        self._get_correct_domain_split()

        # Get the combinations of the member functions
        possibilities = self._get_possibilities()
        combinations = self._get_combinations(possibilities)

        # If we are not running the post processing function after every
        # run, make an appendable list over all the runs which will be
        # passed as an input parameter to the post processing function
        if not(post_process_after_every_run):
            seq_of_dmp_folders = []

        # Print either "now running" or "now submitting"
        self._print_run_or_submit()

        # Set self._len_group if post_processing_function is set, but
        # self._sort_by is None
        if (post_processing_function is not None) and\
           (not(post_process_after_every_run)) and\
           (self._len_group is None):
            # self._len_group is to a number by _get_swapped_input_list
            # (which is called if self._sort_by is not None)
            # If there are no sorting, self._len_group will be None
            # We will make self._len_group the length of the
            # number of runs here
            self._len_group = len(combinations)

        # The run
        for run_no, combination in enumerate(combinations):

            # Get the folder to store the data
            do_run = self._prepare_dmp_folder(combination, **kwargs)
            if not(do_run):
                # Skip this run
                continue

            if remove_old:
                # Remove old data
                self._remove_data()

            # Copy the grid (if any) if cpy_grid files is True
            if (self._cpy_grid) and (self._grid_file is not None):
                combination_list = combination.split()
                # Loop through all the combinations
                for elem in combination_list:
                    # Find the grid
                    if elem[0:4] == "grid":
                        # Remove grid=, so that only the path remains
                        cur_grid = elem.replace("grid=", "")
                        # Copy the grid file
                        shutil.copy2(cur_grid, self._dmp_folder)

            # Check if the run has been performed previously
            do_run = self._check_if_run_already_performed()
            # Do the actual runs
            if do_run:
                # Call the driver for a run
                self._run_driver(combination, run_no)

            # If we would like to call a post_processing function
            if post_processing_function is not None:
                if post_process_after_every_run:
                    # Call the post processing function
                    self._call_post_processing_function(
                        function=post_processing_function,
                        folders=(self._dmp_folder,),
                        **kwargs)
                else:
                    # Append the dmp folder to the list of dmp folders
                    seq_of_dmp_folders.append(self._dmp_folder)
                    # If the run_no+1 is divisible by self._len_group
                    if ((run_no + 1) % self._len_group == 0):
                        # Call the post processing function
                        self._call_post_processing_function(
                            function=post_processing_function,
                            folders=tuple(seq_of_dmp_folders),
                            **kwargs)
                        # Reset the seq_of_dmp_folders
                        seq_of_dmp_folders = []

        # Cast to tuple
        self._PBS_id = tuple(self._PBS_id)
        if hasattr(self._dmp_folders, "__iter__")\
           and not isinstance(self._dmp_folders, str):
            self._dmp_folders = tuple(el for el in self._dmp_folders)
        else:
            self._dmp_folders = (self._dmp_folders,)

        return self._dmp_folders, self._PBS_id

## Nice to have

    def _remove_data(self):
        """
        Removes dmp.*, fail.*, restart.*, log.* and *.cpy files from the
        dump directory
        """

        print("Removing old data")
        remove_extensions = ("dmp.*", "fail.*", "restart.*", "log.*", "cpy")
        files_to_rm = []
        for extension in remove_extensions:
            files_to_rm.extend(
                glob.glob(
                    os.path.join(self._dmp_folder, "*." + extension)))

        # Cast to set (unique values)
        files_to_rm = set(files_to_rm)
        for f in files_to_rm:
            os.remove(f)

        # Remove dirs
        folder_to_rm = glob.glob(
            os.path.join(self._dmp_folder, "before_redistribution_*"))
        folder_to_rm.extend(glob.glob(os.path.join(self._dmp_folder, "run_*")))
        # Filter to only inlcude folders
        folder_to_rm = tuple(f for f in folder_to_rm if os.path.isdir(f))
        for f in folder_to_rm:
            shutil.rmtree(f)

    def _check_if_run_already_performed(self,
                                        restart_file_search_reason=None):
        """
        Checks if the run has been run previously.

        Parameters
        ----------
        restart_file_search_reason : ["restart_from" | "redistribute" | None ]
            Reason to check for restart files if not None.

        Returns
        -------
        bool : [True|False]
            If true is returned, the run will be performed, if not the
            run will not be performed
        """

        dmp_files = glob.glob(os.path.join(self._dmp_folder, "*.dmp.*"))

        if restart_file_search_reason:
            restart_files =\
                glob.glob(os.path.join(self._dmp_folder, "*.restart.*"))
            # Check if dmp or restart files are found
            if len(dmp_files) != 0 or len(restart_files) != 0:
                message = ("Restart or dmp files was found in {}"
                           " when {}"
                           " was set. Run skipped.").\
                    format(self._dmp_folder, restart_file_search_reason)
                _warning_printer(message)
                self._warnings.append(message)
                return False
            else:
                return True
        # Check if dmp files are found if restart is None
        elif len(dmp_files) != 0 and self._restart is None:
            print("Skipping the run as *.dmp.* files was found in "
                  + self._dmp_folder)
            print(("To overwrite old files, run with"
                   " self.execute_runs(remove_old=True)\n"))
            return False
        else:
            return True

# Static helpers

## General helpers
# FIXME: Use pathlib
def _create_folder(folder):
    """Creates a folder if it doesn't exists"""

    if not os.path.exists(folder):
        os.makedirs(folder)
        print(folder + " created\n")

### Printing purposes
def _message_chunker(message, chunk=76):
    """Generator used to chop a message so it doesn't exceed some
    width"""

    for start in range(0, len(message), chunk):
        yield message[start:start + chunk]


def _warning_printer(message):
    """Function for printing warnings"""

    print("{}{}WARNING{}".format("\n" * 3, "*" * 37, "*" * 36))
    # Makes sure that no more than 80 characters are printed out at
    # the same time
    for chunk in _message_chunker(message):
        rigth_padding = " " * (76 - len(chunk))
        print("* {}{} *".format(chunk, rigth_padding))
    print("*" * 80 + "\n" * 3)

### Error checkers
def _check_if_set_correctly(_errors, var=None,
                            possibilities=None):
    """
    Check if a variable is set to a possible variable.
    Called by the error checkers
    """

    # Set a variable which is False if the test fails
    success = True

    # Due to the check done in check_for_correct_type: If the
    # variable is not a string it will be an iterable
    if not isinstance(var[0], str):
        for elem in var[0]:
            # Check if the element is contained in the possibilities
            if not(elem in possibilities):
                success = False
    else:
        # The variable was a string
        if not(var[0] in possibilities):
            success = False

    if not(success):
        message = ("{} was not set to a possible option.\n"
                   "The possibilities are \n{}").\
            format(var[1], "\n".join(possibilities))
        _errors.append("TypeError")
        raise TypeError(message)


def _check_for_correct_type(_errors, var=None,
                            the_type=None,
                            allow_iterable=None):
    """
    Checks if a variable has the correct type

    Parameters
    ----------
    var : tuple
        var[0] - the variable (a data member)

        var[1] - the name of the variable given as a string
    the_type : type
        The data type to be checked
    allow_iterable : bool
        If an iterable with the element as type is allowed
    """

    # Set a variable which is False if the test fails
    success = True
    for cur_var in var:
        # There is an option that the variable could be set to None,
        # and that the default value from BOUT.inp will be used
        if cur_var[0] is not None:
            # Check for the correct type
            if isinstance(cur_var[0], the_type) == False:
                # Check if it is an iterable if iterables are
                # allowed
                if allow_iterable and\
                   hasattr(cur_var[0], "__iter__") and\
                   not isinstance(cur_var[0], dict):
                    for elem in cur_var[0]:
                        # Check for the correct type
                        if isinstance(elem, the_type) == False:
                            success = False
                else:
                    # Neither correct type, nor iterable
                    success = False
            if not(success):
                message = ("{} is of wrong type\n"
                           "{} must be {}").\
                    format(cur_var[1], the_type.__name__)
                if allow_iterable:
                    # If iterable is allowed, then add this
                    message += (" or an iterable with {}"
                                " as elements.").format(the_type.__name__)
                _errors.append("TypeError")
                raise TypeError(message)


def _check_for_child_class_errors(
        remove_old,
    post_processing_function,
    post_process_after_every_run
):
    """
    Function which check for errors in a child class.

    Here a virtual function
    """
    pass


def _error_check_additional(_errors, input_member):
    """
    Checks that the input_member is on the following form:

    >>> input_member = ((section1, name1, (value1-1, value1-2, ...)),
                       (section2, name2, (value2-1, value2-2, ...)),
                       ...)

    Parameters
    ----------
    input member: [self._additional | self._series_add]
        input_member[0] is the input data and
        input_member[1] is the name of the input data
    """

    # If input_member is set
    if input_member[0] is not None:
        # Set a success variable that will fail if anything goes
        # wrong
        success = True

        # Loop through all elements in input_member
        for elem in input_member[0]:
            # Check if self._addition is iterable, but not a string
            # or dict
            if (hasattr(elem, "__iter__")) and\
               (not isinstance(elem, str)) and\
               (not isinstance(elem, dict)):
                if isinstance(elem[0], str):
                    # Check that the second element (the name) is a
                    # string
                    if not isinstance(elem[1], str):
                        success = False
                    # If more than three elements are given
                    if len(elem) != 3:
                        success = False
                # elem[0] is not a string
                else:
                    success = False
            # elem is not iterable or is a dict or a string
            else:
                success = False
        if not(success):
            message =\
                ("{0} is on the wrong form.\n"
                 "{0} should be on the form\n"
                 "{0}=\ \n"
                 "    ((section1, name1, (value1-1, value1-2,...)),\ \n"
                 "     (section2, name2, (value2-1, value2-2,...)),\ \n"
                 "       ...))\n").format(input_member[1])
            _errors.append("TypeError")
            raise TypeError(message)


def _error_check_for_run_input(_errors, _restart, remove_old,
                               post_processing_function,
                               post_process_after_every_run
                               ):
    """
    Check if there are any type errors in input for the run function
    """

    check_if_bool = (
        (remove_old, "remove_old"),
    )

    _check_for_correct_type(_errors, var=check_if_bool,
                            the_type=bool)

    if remove_old and _restart is not None:
        _errors.append("RuntimeError")
        raise RuntimeError("You should not remove old data if you"
                           " want a restart run")

    if (post_processing_function is not None) and\
       (not(hasattr(post_processing_function, "__call__"))):
        _errors.append("RuntimeError")
        message = ("post_process_after_every_run must be a"
                   " function")
        raise RuntimeError(message)

    if (post_process_after_every_run and
        post_processing_function is None):
        _errors.append("RuntimeError")
        message = ("post_process_after_every_run can only be set if"
                   " post_processing_function is given")
        raise RuntimeError(message)

    if (post_process_after_every_run is not None) and\
       (not isinstance(post_process_after_every_run, bool)):
        _errors.append("RuntimeError")
        message = ("post_process_after_every_run must be set to"
                   " a boolean when set")
        raise RuntimeError(message)

    # Check for errors in a child class
    _check_for_child_class_errors(
        remove_old,
        post_processing_function,
        post_process_after_every_run
    )

    def _check_for_basic_instance_error(self):
        """Check if there are any type errors when creating the object"""

        if not isinstance(self._nproc, int):
            message = ("nproc is of wrong type\n"
                       "nproc must be given as an int")
            self._errors.append("TypeError")
            raise TypeError(message)

        if not isinstance(self._directory, str):
            message = ("directory is of wrong type\n"
                       "directory must be given as a str")
            self._errors.append("TypeError")
            raise TypeError(message)

        # Check if MXG and MYG is given as a single int
        # One should not be allowed to specify MXG and MYG as an
        # iterable, as MXG is used to find the correct split, and
        # because it in principle could be incompatible with the method
        # (first, second etc.) used
        check_if_int = (
            (self._MXG, "MXG"),
            (self._MYG, "MYG"),
        )
        _check_for_correct_type(self._errors, var=check_if_int,
                                the_type=int,
                                allow_iterable=False)

        # Check if there are any BOUT.inp files in the self._directory
        inp_file = glob.glob(os.path.join(self._directory, "BOUT.inp"))
        if len(inp_file) == 0:
            self._errors.append("RuntimeError")
            raise RuntimeError("No BOUT.inp files found in '{}'".
                               format(self._directory))

        if self._grid_file is not None:
            # Set a variable which is has length over one if the test fails
            not_found = []
            if isinstance(self._grid_file, str):
                # See if the grid_file can be found
                grid_file = glob.glob(self._grid_file)
                # The grid_file cannot be found
                if len(grid_file) == 0:
                    not_found.append(self._grid_file)
            # If several grid files are given
            elif hasattr(self._grid_file, "__iter__"):
                for elem in self._grid_file:
                    # See if the grid_file can be found
                    grid_file = glob.glob(elem)
                    # The grid_file cannot be found
                    if len(grid_file) == 0:
                        not_found.append(elem)
            if len(not_found) > 0:
                message = ("The following grid files were not found\n"
                           "{}".format("\n".join(not_found)))
                self._errors.append("RuntimeError")
                raise RuntimeError(message)
            if (self._sort_by is not None) and ("grid_file" in self._sort_by):
                # Set a success flag
                success = True
                # The start name of the files
                start_name = "grid_file"
                # Check if grid file is iterable
                if hasattr(self._grid_file, "__iter__"):
                    for grid in grid_file:
                        if grid[0:len(start_name)] != start_name:
                            success = False
                else:
                    # Only one grid file
                    if self._grid_file[0:len(start_name)] != start_name:
                        success = False
                if not(success):
                    message = ("The name of the grid file must start with"
                               " 'grid_file' in order to sort by them.")
                    self._errors.append("RuntimeError")
                    raise RuntimeError(message)


        check_if_int = (
            (self._nx, "nx"),
            (self._ny, "ny"),
            (self._nz, "nz"),
            (self._zperiod, "zperiod"),
            (self._nout, "nout"),
            (self._mxstep, "mxstep"),
            (self._ixseps1, "ixseps1"),
            (self._ixseps2, "ixseps2"),
            (self._jyseps1_1, "jyseps1_1"),
            (self._jyseps1_2, "jyseps1_2"),
            (self._jyseps2_1, "jyseps2_1"),
            (self._jyseps2_2, "jyseps2_2"),
        )

        _check_for_correct_type(self._errors, var=check_if_int,
                                the_type=int,
                                allow_iterable=True)

        # Check if the following is a number
        check_if_number = (
            (self._timestep, "timestep"),
            (self._zmin, "zmin"),
            (self._zmax, "zmax"),
            (self._dx, "dx"),
            (self._dy, "dy"),
            (self._dz, "dz"),
            (self._atol, "atol"),
            (self._rtol, "rtol")
        )

        _check_for_correct_type(self._errors, var=check_if_number,
                                the_type=Number,
                                allow_iterable=True)

        # Check if instance is string, or an iterable containing strings
        check_if_string = (
            (self._solver, "solver"),
            (self._grid_file, "grid_file"),
            (self._ddx_first, "ddx_first"),
            (self._ddx_second, "ddx_second"),
            (self._ddx_upwind, "ddx_upwind"),
            (self._ddx_flux, "ddx_flux"),
            (self._ddy_first, "ddy_first"),
            (self._ddy_second, "ddy_second"),
            (self._ddy_upwind, "ddy_upwind"),
            (self._ddy_flux, "ddy_flux"),
            (self._ddz_first, "ddz_first"),
            (self._ddz_second, "ddz_second"),
            (self._ddz_upwind, "ddz_upwind"),
            (self._ddz_flux, "ddz_flux"),
            (self._sort_by, "sort_by")
        )

        _check_for_correct_type(self._errors, var=check_if_string,
                                the_type=str,
                                allow_iterable=True)

        # Check if the solver is possible
        # From /include/bout/solver.hxx
        possible_solvers = (
            "cvode",
            "pvode",
            "ida",
            "petsc",
            "slepc",
            "karniadakis",
            "rk4",
            "euler",
            "rk3ssp",
            "power",
            "arkode",
            "imexbdf2",
            "snes",
            "rkgeneric",
        )

        # Do the check if the solver is set
        if self._solver is not None:
            _check_if_set_correctly(self._errors, var=(self._solver, "solver"),
                                    possibilities=possible_solvers)

        # Check if ddx or ddy is possible
        possible_method = [
            "C2",
            "C4",
        ]

        # Make a tuple of the variables
        the_vars = (
            (self._ddx_first, "ddx_first"),
            (self._ddx_second, "ddx_second"),
            (self._ddy_first, "ddy_first"),
            (self._ddy_second, "ddy_second")
        )

        for var in the_vars:
            # Do the check if the method is set
            if var[0] is not None:
                _check_if_set_correctly(self._errors, var=var,
                                        possibilities=possible_method)

        # Check if ddz is possible
        possible_method.append("FFT")

        # Make a tuple of the variables
        the_vars = (
            (self._ddz_first, "ddz_first"),
            (self._ddz_second, "ddz_second")
        )

        for var in the_vars:
            # Do the check if the method is set
            if var[0] is not None:
                _check_if_set_correctly(self._errors, var=var,
                                        possibilities=possible_method)

        # Check for upwind terms
        possible_method = (
            "U1",
            "U2",
            "U4",
            "W2",
            "W3",
        )

        # Make a tuple of the variables
        the_vars = (
            (self._ddx_upwind, "ddx_upwind"),
            (self._ddy_upwind, "ddy_upwind"),
            (self._ddz_upwind, "ddz_upwind")
        )

        for var in the_vars:
            # Do the check if the method is set
            if var[0] is not None:
                _check_if_set_correctly(self._errors, var=var,
                                        possibilities=possible_method)

        # Check for flux terms
        possible_method = (
            "SPLIT",
            "NND"
        )

        # Make a tuple of the variables
        the_vars = (
            (self._ddx_flux, "ddx_flux"),
            (self._ddy_flux, "ddy_flux"),
            (self._ddz_flux, "ddz_flux")
        )

        for var in the_vars:
            # Do the check if the method is set
            if var[0] is not None:
                _check_if_set_correctly(self._errors, var=var,
                                        possibilities=possible_method)

        # Appendable list
        possible_sort_by = []

        # Append the 1st element of sort_checks if the 0th elements of
        # sort_checks is not None
        sort_checks = (
            (self._nx, "spatial_domain"),
            (self._ny, "spatial_domain"),
            (self._nz, "spatial_domain"),
            (self._dx, "spatial_domain"),
            (self._dy, "spatial_domain"),
            (self._dz, "spatial_domain"),
            (self._ixseps1, "spatial_domain"),
            (self._ixseps2, "spatial_domain"),
            (self._jyseps1_1, "spatial_domain"),
            (self._jyseps1_2, "spatial_domain"),
            (self._jyseps2_1, "spatial_domain"),
            (self._jyseps2_2, "spatial_domain"),
            (self._symGlobX, "spatial_domain"),
            (self._symGlobY, "spatial_domain"),
            (self._timestep, "temporal_domain"),
            (self._nout, "temporal_domain"),
            (self._solver, "solver"),
            (self._mms, "solver"),
            (self._atol, "solver"),
            (self._rtol, "solver"),
            (self._mxstep, "solver"),
            (self._ddx_first, "ddx_first"),
            (self._ddx_second, "ddx_second"),
            (self._ddx_upwind, "ddx_upwind"),
            (self._ddx_flux, "ddx_flux"),
            (self._ddy_first, "ddy_first"),
            (self._ddy_second, "ddy_second"),
            (self._ddy_upwind, "ddy_upwind"),
            (self._ddy_flux, "ddy_flux"),
            (self._ddz_first, "ddz_first"),
            (self._ddz_second, "ddz_second"),
            (self._ddz_upwind, "ddz_upwind"),
            (self._ddz_flux, "ddz_flux"),
            (self._grid_file, "grid_file")
        )

        for sort_check in sort_checks:
            if sort_check[0] is not None:
                if not(sort_check[1] in possible_sort_by):
                    possible_sort_by.append(sort_check[1])

        # Append the additionals and series_add
        # If additional is set
        if self._additional is not None:
            for additional in self._additional:
                # The additional now contains a tuple of three elements
                # We would like to extract the section (if any) and variable
                # and append them to the possibilities list
                # If the section is empty
                if additional[0] == "":
                    section = ""
                else:
                    section = additional[0] + ":"
                possible_sort_by.append(section + additional[1])
        # Do the same for series_add
        if self._series_add is not None:
            for series_add in self._series_add:
                if series_add[0] == "":
                    section = ""
                else:
                    section = series_add[0] + ":"
                possible_sort_by.append(section + series_add[1])

        # Make a tuple of the variables
        the_vars = (
            (self._sort_by, "sort_by"),
        )

        for var in the_vars:
            # Do the check if the method is set
            if var[0] is not None:
                _check_if_set_correctly(self._errors, var=var,
                                        possibilities=possible_sort_by)

        if self._restart is not None:
            if not isinstance(self._restart, str):
                self._errors.append("TypeError")
                raise TypeError("restart must be set as a string when set")

        possible_method = (
            "overwrite",
            "append"
        )

        # Make a tuple of the variables
        the_vars = (
            (self._restart, "restart"),
        )

        for var in the_vars:
            # Do the check if the method is set
            if var[0] is not None:
                _check_if_set_correctly(self._errors, var=var,
                                        possibilities=possible_method)

        if self._restart_from is not None:
            # Throw warning if restart is None
            if self._restart is None:
                message = "restart_from will be ignored as restart = None"
                _warning_printer(message)
                self._warnings.append(message)

            if not isinstance(self._restart_from, str)\
                    and not(hasattr(self._restart_from, "__call__")):
                self._errors.append("TypeError")
                message = ("restart_from must be set as a string or a "
                           "function returning the restart path when set")
                raise TypeError(message)

        if self._redistribute is not None:
            # Throw warning if restart is None
            if self._restart is None:
                message = "redistribute will be ignored as restart = None"
                _warning_printer(message)
                self._warnings.append(message)
            # Throw a warning if restart is append
            elif self._restart == "append":
                message = ("redistribute is not None and restart = 'append' is"
                           " currently incompatible, setting restart to"
                           " 'overwrite'")
                if not(self._restart_from):
                    message += " (previous files will be saved)"
                _warning_printer(message)
                self._warnings.append(message)
                self._restart = "overwrite"
            if not isinstance(self._redistribute, int):
                self._errors.append("TypeError")
                message = "redistribute must be set as an integer when set"
                raise TypeError(message)
            # If nproc is set, and this is incompatible with NPES
            if self._nproc != self._redistribute:
                raise RuntimeError("nproc and redistribute must be equal")

        if self._restart is not None and self._max_proc is not None:
            if not isinstance(self._max_proc, int):
                message = ("max_proc is of wrong type\n"
                           "max_proc must be given as an int")
                self._errors.append("TypeError")
                raise TypeError(message)

        if self._restart is not None and self._intrp_method is not None:
            if not isinstance(self._intrp_method, str):
                message = ("intrp_method is of wrong type\n"
                           "intrp_method must be given as a string")
                self._errors.append("TypeError")
                raise TypeError(message)

        if self._add_noise is not None:
            # Throw warning if restart is None
            if self._restart is None:
                message = "add_noise will be ignored as restart = None"
                _warning_printer(message)
                self._warnings.append(message)

            raise_error = False
            is_key_none = False
            if isinstance(self._add_noise, dict):
                for var, scale in self._add_noise.items():
                    if not isinstance(var, str):
                        if var is not(None):
                            raise_error = True
                            break
                        else:
                            is_key_none = True
                    if not(isinstance(scale, Number) or (scale is None)):
                        raise_error = True
                        break
                if is_key_none and len(self._add_noise.keys()) > 1:
                    raise_error = True
            else:
                raise_error = True

            if raise_error:
                self._errors.append("TypeError")
                message = ("add_noise must be on the form "
                           "{'var1': number_or_none,"
                           " 'var2': number_or_none, ...}'\n"
                           "or\n"
                           "{None: number_or_none}"
                           )
                raise TypeError(message)

        if self._grid_file is not None:
            # Check if the following variables are found in the grid
            # file
            check_if_in_grid = (
                (self._nx, "nx"),
                (self._ny, "ny"),
                (self._nz, "nz"),
                (self._dx, "dx"),
                (self._dy, "dy"),
                (self._dz, "dz"),
                (self._MXG, "MXG"),
                (self._MYG, "MYG"),
                (self._NXPE, "NXPE"),
                (self._NYPE, "NYPE"),
                (self._ixseps1, "ixseps1"),
                (self._ixseps2, "ixseps2"),
                (self._jyseps1_1, "jyseps1_1"),
                (self._jyseps1_2, "jyseps1_2"),
                (self._jyseps2_1, "jyseps2_1"),
                (self._jyseps2_2, "jyseps2_2"),
                (self._symGlobX, "symmmetricGlobalX"),
                (self._symGlobY, "symmmetricGlobalY")
            )
            for var in check_if_in_grid:
                # If the variable is set
                if var[0] is not None:
                    # Loop through the grid files
                    for grid_file in self._grid_file:
                        # Open (and automatically close) the grid files
                        f = DataFile(grid_file)
                        # Search for mesh data in the grid file
                        grid_variable = f.read(var[1])
                        # If the variable is found
                        if grid_variable is not None:
                            self._errors.append("TypeError")
                            message = ("{0} was specified both in the "
                                       "driver and in the grid file.\n"
                                       "Please remove {}"
                                       " from the driver if you would "
                                       "like to run with a grid file.")
                            raise TypeError(message.format(var[1]))

        if self._grid_file is not None:
            # Make a dict of appendable lists
            spatial_domain = {"nx": [], "ny": [], "nz": []}
            for grid_file in self._grid_file:
                # Open (and automatically close) the grid files
                f = DataFile(grid_file)
                # Search for nx, ny and nz in the grid file
                mesh_types = ("nx", "ny", "nz")
                for mesh_type in mesh_types:
                    grid_variable = f.read(mesh_type)
                    # If the variable is found
                    if grid_variable is not None:
                        spatial_domain[mesh_type].append(grid_variable)
            # Check that the lengths of nx, ny and nz are the same
            # unless they are not found
            len_nx = len(spatial_domain["nx"])
            len_ny = len(spatial_domain["ny"])
            len_nz = len(spatial_domain["nz"])
            if len_nx != 0:
                self._nx = spatial_domain["nx"]
            if len_ny != 0:
                self._ny = spatial_domain["ny"]
            if len_nz != 0:
                self._nz = spatial_domain["nz"]

        if self._nx is not None and self._ny is not None:
            self._check_if_same_len((self._nx, "nx"), (self._ny, "ny"))
        if self._nx is not None and self._nz is not None:
            self._check_if_same_len((self._nx, "nx"), (self._nz, "nz"))
        if self._ny is not None and self._nz is not None:
            self._check_if_same_len((self._ny, "ny"), (self._nz, "nz"))

        if self._nx is not None and self._NXPE is not None:
            self._check_if_same_len((self._nx, "nx"), (self._NXPE, "NXPE"))
        if self._ny is not None and self._NXPE is not None:
            self._check_if_same_len((self._ny, "ny"), (self._NXPE, "NXPE"))
        if self._nz is not None and self._NXPE is not None:
            self._check_if_same_len((self._nz, "nz"), (self._NXPE, "NXPE"))

        if self._nx is not None and self._NYPE is not None:
            self._check_if_same_len((self._nx, "nx"), (self._NYPE, "NYPE"))
        if self._ny is not None and self._NYPE is not None:
            self._check_if_same_len((self._ny, "ny"), (self._NYPE, "NYPE"))
        if self._nz is not None and self._NYPE is not None:
            self._check_if_same_len((self._nz, "nz"), (self._NYPE, "NYPE"))

        if (self._zperiod is not None and
                (self._zmin is not None or self._zmax is not None)):
            self._errors.append("TypeError")
            message = "zperiod and zmin or zmax cannot be set simultaneously."
            raise TypeError(message)
        elif (self._dz is not None and
              (self._zmin is not None or self._zmax is not None)):
            self._errors.append("TypeError")
            message = "dz and zmin or zmax cannot be set simultaneously."
            raise TypeError(message)
        elif (self._zperiod is not None and self._dz):
            self._errors.append("TypeError")
            message = "dz and zperiod cannot be set simultaneously."
            raise TypeError(message)

        # dz is currently set throught zmin and zmax
        if self._dz is not None:
            self._errors.append("TypeError")
            message = ("dz can currently just be set through zmin and zmax\n"
                       "dz = 2*pi*(zmax-zmin)/(MZ)")
            raise TypeError(message)

        if self._dx is not None and self._dy is not None:
            self._check_if_same_len((self._dx, "dx"), (self._dy, "dy"))
        if self._dx is not None and self._dz is not None:
            self._check_if_same_len((self._dx, "dx"), (self._dz, "dz"))
        if self._dy is not None and self._dz is not None:
            self._check_if_same_len((self._dy, "dy"), (self._dz, "dz"))

        if self._dx is not None and self._nx is not None:
            self._check_if_same_len((self._dx, "dx"), (self._nx, "nx"))
        if self._dy is not None and self._ny is not None:
            self._check_if_same_len((self._dy, "dy"), (self._ny, "ny"))
        if self._nz is not None and self._dz is not None:
            self._check_if_same_len((self._dz, "dz"), (self._nz, "nz"))

        if self._timestep is not None and self._nout is not None:
            self._check_if_same_len((self._timestep, "timestep"),
                                    (self._nout, "nout"))

        _error_check_additional(self._errors, (self._additional, "additional"))
        _error_check_additional(self._errors, (self._series_add, "series_add"))

        if self._series_add is not None:
            # Make the second indices iterable if they are not already
            # Start by converting to list, so that self._series becomes
            # modifyable
            self._series_add = list(list(el) for el in self._series_add)
            for index in range(len(self._series_add)):
                if not(hasattr(self._series_add[index][2], "__iter__")):
                    self._series_add[index][2] = (self._series_add[index][2],)
            # Conver to tuple
            self._series_add = tuple(tuple(el) for el in self._series_add)

            # Collect all second indices
            third_indicies = tuple(elems[2] for elems in self._series_add)
            # Find the length of the second indices
            lengths = tuple(
                len(elem) for elem in third_indicies if (
                    not isinstance(
                        elem, str) and not isinstance(
                        elem, dict)))

            # Check that the length of the second indices are the same
            # L.count(value) -> integer -- return number of occurrences
            # of value
            # stackoverflow.com/questions/3844801/check-if-all-elements-in-a-list-are-identical
            if not(lengths.count(lengths[0]) == len(lengths)):
                message = ("The length of the third index of the elements"
                           " of series_add must be the same")
                self._errors.append("TypeError")
                raise TypeError(message)

        #   allow_size_mod is bool
        check_if_bool = (
            (self._mms, "mms"),
            (self._symGlobX, "symGlobX"),
            (self._symGlobY, "symGlobY"),
            (self._cpy_source, "cpy_source"),
            (self._cpy_grid, "cpy_grid"),
            (self._use_expand, "use_expand"),
            (self._allow_size_modification, "allow_size_modification")
        )

        _check_for_correct_type(self._errors, var=check_if_bool,
                                the_type=bool)

        if (self._grid_file is None) and (self._cpy_grid):
            # Raise error
            self._errors.append("TypeError")
            message = ("Cannot copy the grid files if none exists in "
                       " 'grid_file'")
            raise TypeError(message)

        if (self._zmin is not None) and (self._zmax is not None):
            self._check_if_same_len((self._zmin, "zmin"),
                                    (self._zmax, "zmax"))

