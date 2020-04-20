## Copying bash_rc
echo "Copying bashrc file.."
curl https://gitlab.com/gitlab-data/analytics/raw/master/admin/make_life_easier.sh > ~/.bashrc
echo "Copied successfully"

## install homebrew
# Check if exists
command -v brew >/dev/null 2>&1 || { echo "Installing Homebrew.."
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
  } >&2;
echo "Homebrew successfully installed"

## install git
echo "Installing git.."
brew install git
echo "git successfully installed"

## install docker and co
echo "Installing docker.."
brew cask install docker
brew install docker-compose docker-machine xhyve docker-machine-driver-xhyve
sudo chown root:wheel $(brew --prefix)/opt/docker-machine-driver-xhyve/bin/docker-machine-driver-xhyve
sudo chmod u+s $(brew --prefix)/opt/docker-machine-driver-xhyve/bin/docker-machine-driver-xhyve
echo "docker successfully installed"

## install git completion
echo "Installing git completion.."
curl https://raw.githubusercontent.com/git/git/master/contrib/completion/git-completion.bash > ~/.git-completion.bash
echo "git completion successfully installed"

## install iterm2
echo "Installing iTerm2.."
cd ~/Downloads
curl https://iterm2.com/downloads/stable/iTerm2-3_1_7.zip > iTerm2.zip
unzip iTerm2.zip &> /dev/null
mv iTerm.app/ /Applications/iTerm.app
spctl --add /Applications/iTerm.app
rm -rf iTerm2.zip
echo "iTerm2 successfully installed.. Adding colors.."
cd ~/Downloads
mkdir -p ${HOME}/iterm2-colors
cd ${HOME}/iterm2-colors
curl https://github.com/mbadolato/iTerm2-Color-Schemes/zipball/master > iterm2-colors.zip
unzip iterm2-colors.zip
rm iterm2-colors.zip
echo "iTerm2 + Colors installed"

## install visual studio code
echo "Installing VS Code.."
brew cask install visual-studio-code
## this might ask you for your password
code --version
echo "VS Code successfully installed"

## install tldr https://tldr.sh/
echo "Installing tldr..."
brew install tldr
echo "tldr installed. "


## Get oh my zsh (plugins, themes for zsh).
sh -c "$(curl -fsSL https://raw.github.com/robbyrussell/oh-my-zsh/master/tools/install.sh)"
git clone https://github.com/zsh-users/zsh-autosuggestions ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-autosuggestions
# Set zsh theme
sed -i '' 's/ZSH_THEME=".*"/ZSH_THEME="bira"/g' ~/.zshrc
sed -i '' 's/plugins=(git)/plugins=(git zsh-autosuggestions jump)"/g' ~./zshrc

## install the project
echo "Installing the analytics project.."
mkdir ~/repos/
cd ~/repos/
git clone git@gitlab.com:gitlab-data/analytics.git
mark analytics
echo "Analytics repo successfully installed"

## you can now type "jump analytics" and you're in the right place

## gl_open is now an alias to open this on gitlab.com
## install dbt
echo "Installing dbt.."
brew update
brew tap fishtown-analytics/dbt
brew install dbt
echo "dbt successfully installed.. Printing version.."
dbt --version
echo "Setting up dbt profile.."
mkdir ~/.dbt
touch ~/.dbt/profiles.yml
curl https://gitlab.com/gitlab-data/analytics/raw/master/admin/sample_profiles.yml >> ~/.dbt/profiles.yml
echo "dbt profile created.. You will need to edit this file later."
## you will need to edit this file

## install the dbt completion script
echo "Installing dbt completion script.."
curl https://raw.githubusercontent.com/fishtown-analytics/dbt-completion.bash/master/dbt-completion.bash > ~/.dbt-completion.bash
echo "dbt completion script successfully installed"

## Add refresh command
echo "alias dbt_refresh='dbt clean ; dbt deps ; dbt seed'" >> ~/.bash_profile

## install anaconda
echo "Installing anaconda.."
brew cask install anaconda
echo "export PATH=/usr/local/anaconda3/bin:"$PATH"" >> ~/.zshrc
echo "anaconda installed succesfully"

## Set up the computer to contribute to the handbook
echo "Setting up your computer to contribute to the handbook..."
cd ~/repos/
git clone git@gitlab.com:gitlab-com/www-gitlab-com.git
echo "Handbook project successfully installed"
echo "Installing nvm.."
nvm install
nvm use
echo "Installing yarn.."
brew install yarn
echo "Installing rbenv.."
brew install rbenv

# Get ruby version from repo
ruby_version=$(curl -L 'https://gitlab.com/gitlab-com/www-gitlab-com/-/raw/master/.ruby-version' )

rbenv init
rbenv install $ruby_version
rbenv local $ruby_version
gem install bundler
bundle install
##
echo 'eval "$(rbenv init -)"' >> ~/.zshrc
echo 'eval "$(rbenv version")"'


## install iterm2
echo "Installing iTerm2.."
cd ~/Downloads
curl https://iterm2.com/downloads/stable/iTerm2-3_3_9.zip > iTerm2.zip
unzip iTerm2.zip &> /dev/null
mv iTerm.app/ /Applications/iTerm.app
spctl --add /Applications/iTerm.app
rm -rf iTerm2.zip
echo "iTerm2 successfully installed.. Adding colors.."

cd ~/Downloads
mkdir -p ${HOME}/iterm2-colors
cd ${HOME}/iterm2-colors
curl https://github.com/mbadolato/iTerm2-Color-Schemes/zipball/master > iterm2-colors.zip
unzip iterm2-colors.zip
rm iterm2-colors.zip
echo "iTerm2 + Colors installed"

echo "export SNOWFLAKE_TRANSFORM_WAREHOUSE=ANALYST_XS" >> ~/.bash_profile
echo "export SNOWFLAKE_LOAD_DATABASE=RAW" >> ~/.bash_profile
echo "export SNOWFLAKE_SNAPSHOT_DATABASE='SNOWFLAKE'" >> ~/.bash_profile
echo "source ~/.bashrc" >> ~/.bash_profile
echo "source ~/.bashrc"
echo "source ~/.bash_profile"

echo "Onboarding script ran successfully"
